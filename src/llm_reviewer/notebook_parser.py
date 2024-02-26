import json

from fuzzywuzzy import fuzz
from openai import OpenAI
from llama_index.program import OpenAIPydanticProgram
from llama_index.llms.openai import OpenAI as llama_index_OpenAI
from pydantic import BaseModel, Field


from src.llm_reviewer.constants import Roles, PATH_TO_SECRETS

with open(PATH_TO_SECRETS, "r") as f:
    openai_api_key = json.load(f)["openai_api_key"]


def get_closest_match(query, choices):
    """
    Get the closest match(es) to a query string from a list of choices.

    :param query: The query string.
    :param choices: A list of strings to match against.
    :param limit: The maximum number of matches to return.
    """
    best_role = None
    best_score = 0
    for choice in choices:
        score = fuzz.ratio(query, choice)
        if score > best_score and score > 25:
            best_score = score
            best_role = choice

    return best_role, best_score


def count_empty_from_end(cells):
    count = 0
    for message in reversed(cells):
        if message["source"].strip() == "":
            count += 1
        else:
            break
    return count


def extract_messages(notebook):
    """
    Parse a notebook and extract the message objects.

    :param notebook: The notebook object.
    """
    if notebook is None:
        return []

    messages = []
    cut_tail = count_empty_from_end(notebook.cells)
    cells = notebook.cells[2:]
    if cut_tail:
        cells = cells[:-cut_tail]
    for cell in cells:
        if cell["cell_type"] == "markdown":
            headers = ["**User**", "**Assistant**"]
        elif cell["cell_type"] == "code":
            headers = ["# User", "# Assistant"]
        else:
            raise Exception(f'Unknown cell type {cell["cell_type"]}')

        lines = cell["source"].split("\n")
        first_line = lines[0]
        role, score = get_closest_match(first_line, headers)
        if score > 50:
            valid_role = role.replace("*", "").replace("#", "").strip()
            content = "\n".join(lines[1:]).strip("\n")
        else:
            valid_role = ""
            content = cell["source"]
        message = {"role": valid_role, "content": content, "type": cell["cell_type"]}
        messages.append(message)

    return messages


def fix_missing_roles(messages):
    """
    Fix missing roles in a list of messages.

    :param messages: The list of messages.
    """
    def predict_role(messages_subsequence):
        try:
            openai_client = OpenAI(api_key=openai_api_key)
            response = openai_client.chat.completions.create(
                model="gpt-4-1106-preview",
                messages=[
                    {"role":"system", "content": "Your task is to accurately predict whether the empty role is a User or an Assistant. You are only allowed to reply with a single word: 'User' or 'Assistant'."},
                    {"role":"user", "content": f"Here's a part of the conversation including an empty role:\n\n{messages_subsequence}"}
                ],
                temperature=0,
                seed=42
            )
            print(response.choices[0])
            missing_role = response.choices[0].message.content
            assert missing_role in ["User", "Assistant"]
            return missing_role, None
        except Exception as e:
            return None, e

    errors = []
    for i in range(len(messages)):
        if messages[i]["role"] == "":
            subsequence = messages[max(0, i-2):min(len(messages), i+3)]
            messages[i]["role"], error = predict_role(subsequence)
            if error is not None:
                errors.append(error)
    return messages, errors


def extract_metadata(notebook):
    if notebook is None:
        return {}
    
    # # Extract the first cell
    first_cell = notebook.cells[0]
    lines = first_cell["source"].split("\n")
    metadata = {}
    for line in lines:
        if "**Python Topics**" in line:
            metadata["topic"] = line.split(" - ")[1]
        if "**Type**" in line:
            metadata["type"] = line.split(" - ")[1]
        if "**Target Number of Turns (User + Assistant)**" in line:
            metadata["target_turns"] = line.split(" - ")[1]


    if "Project / Action" in first_cell["source"]:
        metadata = parse_metadata_dynamically(first_cell["source"])

    return metadata


def parse_metadata_dynamically(metadata_string): 
    """
    Identify the language of each code block and transform it into markdown syntax highlighting.
    """


    SYSTEM_PROMPT = f"""IDENTITY:
    You are an information processor.

    INSTRUCTION:
    We have metadata strings that contain key/value pairs. We need to identify the key/value pairs within the metadata string and extract it as a JSON object.

    If there can be nested key/value pairs, please provide the JSON object containing only the leaf key/value pairs flattened.
    """
    try:
        openai_client = OpenAI(api_key=openai_api_key)
        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": metadata_string},
            ],
            temperature=0.0,
            max_tokens=256,
            seed = 42,
            response_format={ 
                "type": "json_object" 
            },
        )
        metadata = json.loads(response.choices[0].message.content)
        return metadata
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

  
def transform__concatenate_back_to_back_messages_from_same_role(messages):
    """
    Merge back-to-back user & ai messages into a single message, with the content of the messages concatenated together.
    """
    if len(messages) == 0:
        return []

    concatenated_messages = []
    current_concatenation = ""
    current_role = messages[0]['role']
    for message in messages:
        if message.get('role') == current_role:
            current_concatenation += message.get('content').strip() + "\n\n"
        else:
            concatenated_messages.append({
                'role': current_role,
                'content': current_concatenation.strip()
            })
            current_concatenation = message.get('content') + "\n\n"
            current_role = message.get('role')

    # Add the last concatenation
    concatenated_messages.append({
        'role': current_role,
        'content': current_concatenation.rstrip()
    })
    return concatenated_messages



def transform__code_blocks_to_syntax_highlighted_md(messages): 
    """
    Identify the language of each code block and transform it into markdown syntax highlighting.
    """
    transformed_messages = []

    # Isolate Code blocks
    for message in messages:

        if message.get("type") != "code":
            transformed_messages.append(message)
            continue

        message_copy = message.copy()

        # Identify the language of each code block
        class Language(BaseModel):
            """Data model for identifying the language of a code block for use in markdown syntax highlighting."""
            language: str

        prompt_template_str = """
        Given the following conversation, Identify the language of each code block.
        The output should be compatible with markdown syntax highlighting for triple backtick code blocks.

        Contents:
        {code}
        """
        program = OpenAIPydanticProgram.from_defaults(
            llm=llama_index_OpenAI(api_key=openai_api_key, model="gpt-4-1106-preview", temperature=0),
            output_cls=Language,
            prompt_template_str=prompt_template_str,
            verbose=False,
        )
        output = program(
            code=message_copy.get("content").strip()
        )
        language = output.language

        # Strip leading and trailing spaces/newlines from the code block
        message_copy["content"] = f"```{language}\n{message_copy.get('content').strip()}\n```"
        message_copy["type"] = "markdown"

        # Wrap in triple backticks and append to transformed messages
        transformed_messages.append(message_copy)

    return transformed_messages



def notebook_parser(notebook):
    messages = extract_messages(notebook)
    metadata = extract_metadata(notebook)
    messages, errors = fix_missing_roles(messages)
    messages = transform__code_blocks_to_syntax_highlighted_md(messages)
    messages = transform__concatenate_back_to_back_messages_from_same_role(messages)
    if errors:
        raise Exception("Failed to predict missing roles.")
    return {"metadata": metadata, "messages": messages}


def split_messages_into_turns(messages):
    turns = []
    current_role_steps = []
    if not messages:
        return {
            "status": "ERROR",
            "reason": "No messages were provided to turn splitter.",
        }

    current_role = messages[0]["role"]
    for message in messages:
        role = message["role"]
        if current_role != role:
            turns.append({"role": current_role, "steps": current_role_steps})
            current_role_steps = []
            current_role = role
        current_role_steps.append(
            {"type": message["type"], "content": message["content"]}
        )
    if current_role_steps:
        turns.append({"role": current_role, "steps": current_role_steps})

    for turn in turns:
        if turn["role"] == "Assistant":
            turn["role"] = Roles.LLM.value
        elif turn["role"] == "User":
            turn["role"] = Roles.HUMAN.value
        else:
            return {"status": "ERROR", "reason": "Contains unrecognized header"}

    grouped_turns = []
    for i in range(0, len(turns), 2):
        group = turns[i : i + 2]
        grouped_turns.append(group)
    return {"status": "OK", "turns": grouped_turns}


def notebook_to_turns(notebook):
    parsed_notebook = {**notebook_parser(notebook)}
    turns = split_messages_into_turns(parsed_notebook["messages"])
    if turns["status"] == "OK":
        return turns["turns"]
    else:
        raise Exception("Bad notebook")
