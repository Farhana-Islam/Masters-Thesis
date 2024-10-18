from supporting_functions import train_test_split
import polars as pl
from collections import Counter
import re


train_df, test_df = train_test_split()


# considering rows with tool values
train_df_with_tool = train_df[train_df["tool"].notnull()]


def list_to_string(list_tool):
    new_string = "@".join(list_tool)

    return new_string


def sorted_list(x):
    unique_set = set(x)

    unique_list = list(unique_set)

    sorted_unique_list = sorted(unique_list)

    string_tool = list_to_string(sorted_unique_list)

    return string_tool


def tool_pre_processing(data):
    data.loc[:, ("tool_list")] = data.loc[:, "tool"].apply(
        lambda x: x.lstrip("[").rstrip("]").replace(" ", "").split(",")
    )

    data.loc[:, ("tool_list")] = data["tool_list"].apply(lambda x: sorted_list(x))

    data.loc[:, ("tool_list")] = data["tool_list"].fillna("None")

    return data


def tool_post_processing(data):
    data.loc[:, "tool_extracted"] = data.tool_extracted.apply(lambda x: x.split("@"))

    data.loc[:, "tool_extracted"] = data["tool_extracted"].apply(lambda x: sorted_list(x))

    data.loc[:, "tool_extracted"] = data["tool_extracted"].fillna("None")

    return data


tool_pre_processing(train_df_with_tool)


# this funstion will create a list of all tools used by jobs
def create_tool_list():
    tools = train_df[train_df["tool"].notnull()]["tool"]
    temp_list1 = tools.str.strip("[]").str.split(",")
    temp_list2 = [j for i in temp_list1 for j in i]
    tool_list = [item.lstrip() for item in temp_list2]
    return tool_list


def create_dict_with_number_of_time_tool_used():
    tool_list = create_tool_list()
    tool_dict = Counter(tool_list)
    return tool_dict


tool_dict = create_dict_with_number_of_time_tool_used()


def generate_tool_list(tools):
    """This function creates a list of unique tools used"""
    dict_with_viable_tools = {k: v for (k, v) in tools.items() if v > 200}
    return dict_with_viable_tools


tools = generate_tool_list(tool_dict)


def string_to_list(s):
    """This function takes in a string s, splits the string by special characters,
    removes any spaces and single letters and returns a list of words"""

    split_s = re.split(r"[\W_\d]+", s)
    split_simple = [y.strip() for y in split_s if (y not in ("", " ")) and (len(y) >= 2)]

    return split_simple


def common_words_for_tool(t, df):
    """This function looks for the most common words associated with a tool t in a dataframe df.
    It returns the 25 most common words"""

    # Tools_used is a list of tools. We filter when tool t is in tools_used
    # Then turn the concatenated column of 'command'and 'jobname' into a list of words
    commands = df[df.tool_list.map(set(t).issubset)].job_cmd.apply(lambda x: string_to_list(x)).tolist()
    # print(df[df.tool_list.map(set(t).issubset)].job_cmd)

    # Commands is a list of lists, so we flatten it into one single list of words
    flat_list = [item for sublist in commands for item in sublist]

    # Count the occurrence of each word
    counts = Counter(flat_list)

    return counts.most_common(25)


def intersection(col, t):
    if t in col:
        return True
    else:
        return False


polar_train_df_with_tool = pl.from_pandas(train_df_with_tool)


def relevant_key_words_for_tool_polars(t, df_pandas, df):
    """This function finds the keywords for a tool t in dataframe df.
    It returns a list of keywords"""

    # First we obtain the common words associated with tool t in df
    common = common_words_for_tool(t, df_pandas)

    # If tool t does not appear in df, then we do not search for keywords
    # Simply add t as a keyword
    if not common:
        return [t]

    # Convert the common words into a list
    list_words = list(zip(*common))[0]

    key_words = list()

    for w in list_words:
        # Filter df by when command_jobname contains the word w, and obtain value counts for tool used

        tool_counts = (
            df.filter(pl.col("job_cmd").str.contains(w)).select(pl.col("tool_list")).to_series().value_counts()
        )

        tool_counts = tool_counts.with_columns(
            pl.col("tool_list").apply(lambda x: re.split("@", x)).alias("tools_used")
        )

        # Sum the total count of rows that have w in command_jobname
        # total_counts = tool_counts.sum()
        total_counts = tool_counts.select(pl.col("count")).to_series().sum()
        # Now sum how many of the rows with w in command_jobname used tool t
        tool_counts = tool_counts.with_columns(
            pl.col("tools_used").apply(lambda x: intersection(x, t)).alias("tool_boolean")
        ).filter(
            pl.col("tool_boolean") == True  # noqa: E712
        )

        # t_count = tool_counts.sum()
        t_count = tool_counts.select(pl.col("count")).to_series().sum()

        # If ratio is above 0.85 then set as key_word
        if t_count / total_counts > 0.85:
            key_words.append(w.strip())
    # It is possible that the tool itself will not be a keyword by the above definition
    # But we still consider it to be a keyword of itself so we also add it
    if t not in key_words:
        key_words.append(t)
    return key_words


def key_words_dictionary_polars(df, tools):
    """This function generates a dictionary, where each tool t has a list of keywords."""

    # First generate the list of tools that are found in TR data
    # print('Generating tool list...')
    tool_list = generate_tool_list(tools)

    df = df.with_columns(pl.col("tool_list").apply(lambda x: re.split("@", x)).alias("tools_used"))
    # df = df.with_columns((pl.col("command") + "_" + pl.col("jobname")).alias("command_jobname"))

    df_pandas = df.to_pandas()
    # Then for each tool in the tool list find the relevant key words and add it to dictionary
    print("Writing key words dictionary...")
    tools_key_words = dict((t, relevant_key_words_for_tool_polars(t, df_pandas, df)) for t in tool_list)
    return tools_key_words


new_dict = key_words_dictionary_polars(polar_train_df_with_tool, tool_dict)

# ------------------------------------------------------------------------------------------------------------------------------------
# Creating tool_extracted feature of train and test set based on tool dictionary created above on train set


def tool_extraction_from_command(x, reversed_dict, tool_key_words):
    """This function extracts the tools from the command_jobname string.
    Arguments:
        x: command_jobname string
        reversed_dict: The keyword dictionary reversed, so the keywords are keys and the tool are values
        tool_key_words: The complete list of all keywords found for all tools

    Returns: String containing all tools extracted from command_jobname"""

    # First convert string x into a list of words
    command_simple = string_to_list(x)

    # Find the intersection between the list of words from x and the list of all keywords
    k_list = list(set(command_simple) & set(tool_key_words))

    # For the intersection, find the tools associated with the keywords from the reversed dictionary
    t_list = [reversed_dict[key] for key in k_list if key in tool_key_words]

    # If there are not tools add string 'None'
    if len(t_list) == 0:
        t_list.append("None")

    # Convert the list into a string, with the tools separated by @
    # Having a list type in one of the pandas columns can give some errors when saving and loading the table
    unique_list = list(set(t_list))
    string_list = "@".join(map(str, unique_list))

    return string_list


def tool_extraction(df, tools_dictionary):
    """This function extracts the tools used for each job from the command_jobname field
    Arguments:
        df: dataframe
        tools_dictionary: The dictionary of keywords for each tool

    Returns: dataframe with tool extracted"""

    # reverse the tool dictionary, so the keywords are keys of the dictionary, and the tools are the values
    reversed_dict = {item: key for key, values in tools_dictionary.items() for item in values}
    tool_key_words = list(reversed_dict.keys())  # Obtain full list of keywords

    # df["command_jobname"] = df["command"] + "__" + df["jobname"]  # Concatenate command and jobname column

    print("Extracting tools...")

    df["tool_extracted"] = df["job_cmd"].apply(lambda x: tool_extraction_from_command(x, reversed_dict, tool_key_words))

    print("Done")
    return df


def tool_extract_train_and_test():
    df_with_tool_test = tool_extraction(test_df, new_dict)
    # to sort the tools alphabetically tool_post_processing function is implemented
    df_with_tool_test = tool_post_processing(df_with_tool_test)

    df_with_tool_train = tool_extraction(train_df, new_dict)
    # to sort the tools alphabetically tool_post_processing function is implemented
    df_with_tool_train = tool_post_processing(df_with_tool_train)

    return df_with_tool_train, df_with_tool_test
