import re
from typing import Optional
from googleapiclient.discovery import Resource

import nbformat
import numpy as np
from nbformat.v4 import new_notebook, new_markdown_cell
from scipy.stats import rankdata

import json
from typing import Optional, List, Dict, Set
from googleapiclient.discovery import Resource

import os
from openai import OpenAI

openai_api_key = os.environ.get("OPENAI_API_KEY")
client = OpenAI()

def load_problems(file_path: str) -> List[dict]:
    """
    Load a list of problems from a JSON file.

    Parameters:
    file_path: str
        The path to the JSON file containing problems data.

    Returns:
    list
        A list of problems loaded from the JSON file, or an empty list if the file
        cannot be found or read as valid JSON.
    """
    existing_problems = []
    try:
        with open(file_path, 'r') as f:
            existing_problems = json.load(f)
    except FileNotFoundError:
        print(f"{file_path} not found.")
    except json.JSONDecodeError:
        print(f"{file_path} is not a valid JSON file.")
    
    return existing_problems

def extract_questions(problems: List[dict]) -> Set[str]:
    """
    Extract unique questions from a list of existing problems.

    Parameters:
    problems: list
        The list of problems, each of which is a dictionary that may contain a
        list of messages under the key "messages".

    Returns:
    set
        A set of unique questions asked by users in the problem messages.
    """
    existing_questions = set()

    for problem in problems:
        messages = problem.get("messages", [])
        for message in messages:
            if message.get("role") == "user":
                user_content = message.get("content")
                if user_content:
                    existing_questions.add(user_content)
    
    return existing_questions

def find_and_load_all_problems(parent_folder: str) -> List[dict]:
    """
    Look for problems.json files in each child folder, concatenate the content, and return a list of problems.

    Parameters:
    parent_folder: str
        The path to the parent directory.

    Returns:
    list
        A list of problems loaded from all the problems.json files within the child directories.
    """
    all_problems = []
    for root, dirs, files in os.walk(parent_folder):
        for file in files:
            if file == 'problems.json':
                json_file_path = os.path.join(root, file)
                loaded_problems = load_problems(json_file_path)
                all_problems.extend(loaded_problems)
                
    return all_problems

def extract_questions_by_topic(problems: List[dict]) -> Dict[str, Set[str]]:
    """
    Extract questions grouped by topic from a list of existing problems.

    Parameters:
    problems: list
        The list of problems, each of which is a dictionary with "metadata" and "messages".

    Returns:
    dict
        A dictionary where each key is a topic string, and each value is a set of unique
        questions asked by users in the problem messages pertaining to that topic.
    """
    questions_by_topic = {}

    for problem in problems:
        topic = problem.get("metadata", {}).get("topic", "Unknown Topic")
        messages = problem.get("messages", [])
        for message in messages:
            if message.get("role") == "user":
                user_content = message.get("content")
                if user_content:
                    if topic not in questions_by_topic:
                        questions_by_topic[topic] = set()
                    questions_by_topic[topic].add(user_content)
    
    return questions_by_topic

def process_problems(problems):
    problem_titles = []
    problem_topic_counts = {}
    file_path_to_problem = {}

    for problem in problems:
        # Extract topic type and type from the problem's metadata
        topic_type = f'{problem["metadata"]["topic"].split(" > ")[-1]}__{problem["metadata"]["type"]}'
        idx = problem_topic_counts.get(topic_type, 0)
        
        # Create a unique title for the problem
        title = f'{topic_type}__{idx}'
        modified_title = f'{title}_V3_A'
        problem_titles.append(modified_title)

        # Map the file path to the problem
        file_path_to_problem[f"{modified_title}.ipynb"] = problem
        problem_topic_counts[topic_type] = idx + 1

    return problem_titles, file_path_to_problem, problem_topic_counts

def load_complexity_data(file_path):
    with open(file_path) as f:
        return json.load(f)

def flatten_complexity_scores(data, key_prefix='', scores_dict=None):
    if scores_dict is None:
        scores_dict = {}
    for key, value in data.items():
        if isinstance(value, dict):
            if 'complexity' in value:
                scores_dict[f'{key_prefix}.{key}'.strip('.')] = value['complexity']
            else:
                flatten_complexity_scores(value, f'{key_prefix}.{key}'.strip('.'), scores_dict)
        elif isinstance(value, int):
            scores_dict[key_prefix] = value
    return scores_dict

def sort_and_rank_complexity_scores(complexity_scores):
    sorted_scores = dict(sorted(complexity_scores.items(), key=lambda item: item[1], reverse=True))
    ranks = rankdata(list(sorted_scores.values()), method='average')
    return {topic: rank / len(complexity_scores) for topic, rank in zip(sorted_scores.keys(), ranks)}

def get_turn_range(turns, min_turns=1, max_turns=15, margin=3):
    return max(min_turns, turns - 1), min(max_turns, turns + margin)

def biased_normal_distribution(input_number, min_val=1, max_val=15, std_dev=2.5, base_mean=1):
    mean = np.interp(input_number, [0, 100], [min_val, max_val])
    adjusted_mean = (mean + 3*base_mean) / 4
    while True:
        sample = np.random.normal(adjusted_mean, std_dev)
        if min_val <= sample <= max_val:
            break
    return int(sample)

def create_and_save_notebook(notebook_path, problems, problem_titles, topic_percentiles):
    for p, t in zip(problems, problem_titles):
        metadata_topic_hierarchy = p["metadata"].get("topic") 
        normalized_topic = metadata_topic_hierarchy.replace(" > ", ".")
        percentile = topic_percentiles.get(normalized_topic, 0)
        turns = biased_normal_distribution(percentile * 100)
        min_turns, max_turns = get_turn_range(turns)

        notebook = new_notebook()
        metadata_cell = new_markdown_cell(f"""# Metadata\n\n**Python Topics** - {p["metadata"]["topic"]}\n\n**Type** - {p["metadata"]["type"]}\n\n**Target Number of Turns (User + Assistant)** - {min_turns}-{max_turns}\n""")
        conversation_header_cell = new_markdown_cell("# Conversation")
        conversation_message_cell = new_markdown_cell(f"""**User**\n\n{p["messages"][0]["content"]}\n""")

        notebook.cells.extend([metadata_cell, conversation_header_cell, conversation_message_cell])

        os.makedirs(os.path.dirname(notebook_path), exist_ok=True)
        with open(notebook_path.format(title=t), 'w') as f:
            nbformat.write(notebook, f)

def crawl_keys(d, sep=' > ', prefix=''):
    """
    Recursively crawls through a nested dictionary, collecting paths to all keys.

    This function traverses a nested dictionary and creates a list of paths to each key. 
    The paths are constructed by concatenating keys at each level with a specified separator.

    Parameters:
    - d (dict): The nested dictionary to crawl.
    - sep (str, optional): The separator to use between keys in the path. Defaults to ' > '.
    - prefix (str, optional): A prefix to prepend to each path. Defaults to an empty string.

    Returns:
    - list of str: A list containing the paths to each key in the dictionary.
    """
    paths = []
    for k, v in d.items():
        path = prefix + k
        if isinstance(v, dict) and len(v.keys()) > 0:
            paths.extend(crawl_keys(v, sep, path + sep))
        else:
            paths.append(path)
    return paths

def extract_metadata_from_cell(cell_content):
    """
    Extracts metadata from the given markdown cell content.
    """
    metadata_patterns = {
        'occupation': r'\*\*Occupation Topics\*\* - ([\w\s-]+(?:\(.*?\))?[ \w-]*) > ([\w\s-]+?) - (.+)',
        'turns': r'\*\*Target Number of Turns \(User \+ Assistant\)\*\* - ([\d-]+)',
        'use_case': r'\*\*Use Case\*\* - (.+)',
        'technical_topic': r'\*\*Technical Topic\*\* - (.+)',
        'personality': r'\*\*User Personality\*\* - (.+)'
    }

    metadata = {}
    for key, pattern in metadata_patterns.items():
        match = re.search(pattern, cell_content)
        metadata[key] = match.group(1).strip() if match else "Unknown"

    metadata['experience_level'], metadata['learning_topic'] = parse_occupation_data(metadata['occupation'])
    return metadata

def parse_occupation_data(occupation_data):
    """
    Parses occupation data to extract experience level and learning topic.
    """
    experience_levels = ["First Timer", "Beginner", "Intermediate", "Advanced", "Expert"]
    experience_level = "Unknown"
    learning_topic = occupation_data

    for level in experience_levels:
        if level in occupation_data:
            experience_level = level
            learning_topic = occupation_data.replace(level, '', 1).strip(' -')
            break

    return experience_level, learning_topic

def clean_markdown_cell(cell_source, metadata):
    """
    Cleans and updates the markdown cell with new metadata.
    """
    lines_to_remove = [
        "**Occupation Topics** -",
        "**Target Number of Turns (User + Assistant)** -",
        "**Use Case** -",
        "**Technical Topic** -",
        "**User Personality** -"
    ]

    # Remove unwanted lines
    lines = [line for line in cell_source.split('\n') if not any(line.startswith(prefix) for prefix in lines_to_remove)]

    # Append updated metadata
    updated_metadata = [
        f"**User Occupation** - {metadata['occupation']}",
        f"**User Experience Level** - {metadata['experience_level']}",
        f"**User Use Case** - {metadata['learning_topic']} - {metadata['use_case']}",
        f"**Technical Topic Suggestion** - {metadata['technical_topic']}",
        f"**User Personality** - {metadata['personality']}",
        f"**Target Number of Turns (User + Assistant)** - {metadata['turns']}"
    ]

    return '\n'.join(lines + [line for line in updated_metadata if "Unknown" not in line])

def modify_notebook(notebook_path):
    """
    Modifies the markdown cells of the notebook at the given path.
    """
    with open(notebook_path, 'r', encoding='utf-8') as f:
        notebook = nbformat.read(f, as_version=4)

    for cell in notebook.cells:
        if cell.cell_type == 'markdown':
            metadata = extract_metadata_from_cell(cell.source)
            cell.source = clean_markdown_cell(cell.source, metadata)

    # Save the updated notebook
    with open(notebook_path, 'w', encoding='utf-8') as f:
        nbformat.write(notebook, f)

def process_notebooks_in_folder(folder_path):
    """
    Processes all notebooks in the given folder.
    """
    for filename in os.listdir(folder_path):
        if filename.endswith('.ipynb'):
            modify_notebook(os.path.join(folder_path, filename))
            print(f'Modified notebook: {filename}')
        else:
            print(f'Skipping file: {filename}')
