
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from tqdm import tqdm

sys.path.append('../..')
print(sys.path)


from src.Q_generator.gdrive_upload import (build_service_and_upload_files,
                                           update_problems_with_metadata)
from src.Q_generator.question_generator import QuestionGenerator
from src.Q_generator.question_processor import (
    crawl_keys, create_and_save_notebook, extract_questions_by_topic,
    find_and_load_all_problems, flatten_complexity_scores,
    load_complexity_data, process_notebooks_in_folder, process_problems,
    sort_and_rank_complexity_scores)
from src.sheets_utils import GoogleSheetsService

# Constants
# Keep constants, configs in unified place.Later we can implement it with Config Parser.
QGEN_CONFIG_PATH = './configs/config.json'
TOPIC_PATH = './configs/topic_dist.json'
TOPIC_DIST_PATH = './configs/topic_dist.json'
GSHEET_TITLE_PATTERN = 'Conversations_Batch_'
SERVICE_ACCOUNT_FILE = './creds/google__sa.json'
SPREADSHEET_ID = ''
SPREADSHEET_ID_ME = '1uJ_M1sNdPp0OWrG4q-48n15T8wk80KLr4K7OFyOnBg8'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
PROMPT_PATH = "../../src/Q_generator/prompts/generate_human_like_questions.txt"
QUESTIONS_DIR = './notebooks/'
NEW_BATCH_DIR = ''
GDRIVE_URL = 'https://drive.google.com/drive/u/2/folders/1FuHZZ18qn6k8iXTi5YbstGRr56w0kDEz'
GDRIVE_URL_ME = 'https://drive.google.com/drive/folders/1u8wpM-V7UCCCrRdTSrm8D2zhJfXA3dty'


# Globals
generated_questions_count = 0

def load_config(config_path):
    with open(config_path, "r") as file:
        return json.load(file)


def get_topics(topic_path):
    '''
    Returns a list of all topics in the topic hierarchy
    '''
    print(os.getcwd())
    with open(topic_path) as json_file:
        topic_hierarchy = json.load(json_file)
        all_topics = crawl_keys(topic_hierarchy)
        return all_topics


def get_existing_problems(dir_path):
    all_problems = find_and_load_all_problems(dir_path)
    questions_grouped_by_topics = extract_questions_by_topic(all_problems)
    return questions_grouped_by_topics


def generate_notebook_path(title_name={}, parent_dir="./notebooks", sub_dir="/v3/old_pipe/", extension=".ipynb"):
    """
    Generates a file path pattern for a Jupyter notebook with a placeholder for the title.

    Args:
        title_name (str): The specific title name to be inserted into the file path.
        parent_dir (str): The parent directory for the file path.
        sub_dir (str): The subdirectory path including intermediate folders.
        extension (str): The file extension for the notebook.

    Returns:
        str: A file path pattern with the title inserted.
    """
    # Clean paths to remove any trailing slashes
    parent_dir = parent_dir.rstrip('/')
    sub_dir = sub_dir.strip('/')

    # Construct the file path pattern
    file_path_pattern = f"{parent_dir}/{sub_dir}"
    return file_path_pattern


def get_next_batch_subdir(parent_dir, pattern="batch_"):
    """
    Finds the next batch subdirectory by looking for the highest numbered pattern and incrementing it by 1.

    Args:
        parent_dir (str): The parent directory containing batch subdirectories.
        pattern (str): The pattern prefix for batch subdirectories.

    Returns:
        str: The full path to the next batch subdirectory.
    """
    # Compile a regular expression pattern to match "batch_X" and capture X
    batch_pattern = re.compile(re.escape(pattern) + r'(\d+)')

    # Get all entries in the parent directory
    dir_entries = os.listdir(parent_dir)

    # Find matches for the "batch_X" pattern and extract the numbers
    batch_numbers = [
        int(batch_pattern.search(entry).group(1))
        for entry in dir_entries
        if batch_pattern.search(entry)
    ]

    # Find the highest number and increment it by 1 to get the next batch number
    next_batch_number = max(batch_numbers) + 1 if batch_numbers else 1

    # Construct the new batch directory name
    next_batch_dir = f"{pattern}{next_batch_number}"
    full_path = os.path.join(parent_dir, next_batch_dir)

    return next_batch_dir


def get_max_batch_number(sheet_titles, pattern_prefix="Conversations_Batch_"):
    # A regex to capture the numeric part of sheet titles following the given pattern_prefix
    number_extractor_regex = re.compile(re.escape(pattern_prefix) + r'(\d+)$')

    # Extract numbers from the sheet titles that match the pattern
    numbers = [
        int(match.group(1)) for title in sheet_titles
        for match in [number_extractor_regex.match(title)]
        if match
    ]

    # Return the maximum number if there are any numbers, otherwise return 0
    return max(numbers) + 1 if numbers else 999999


def update_google_spreadsheet(servie_account_cred_path, spreadsheet_id, problems):

    # Usage example
    

    # Create an instance of GoogleSheetsService
    gsheets_service = GoogleSheetsService(SERVICE_ACCOUNT_FILE, SCOPES)
    sheet_names = gsheets_service.get_sheet_titles(spreadsheet_id)
    new_batch_no = get_max_batch_number(
        sheet_names, pattern_prefix="Conversations_Batch_")
    new_sheet_name = "Conversations_Batch_%s" % new_batch_no
    print(new_sheet_name)

    gsheets_service.ensure_sheet_exists(spreadsheet_id, new_sheet_name)

    values_to_update = []
    for problem in problems:
        values_to_update.append([
            problem["metadata"]["colab_url"],
            problem["metadata"]["topic"],
        ])

    gsheets_service.update_or_append_data_to_sheet(
        spreadsheet_id, new_sheet_name, values_to_update)


def generate_questions_in_parallel(questions_grouped_by_topics, all_topics, config, generator_instance):
    '''
    Generates questions for a given topic in parallel
    '''
    # Globals
    global generated_questions_count

    questions_grouped_by_topic = {}
    for topic in all_topics:
        questions_grouped_by_topic[topic] = questions_grouped_by_topics.get(topic, set())
    problems = []
    generated_questions_lock = Lock()

    # Extract configurations
    MAX_QUESTIONS = config['MAX_QUESTIONS']
    MAX_THREADS = config['MAX_THREADS']
    NUMBER_OF_QUESTIONS_TO_GENERATE = config['NUMBER_OF_QUESTIONS_TO_GENERATE']

    # Function to generate questions for a given topic

    def generate_for_topic(topic):
        with generated_questions_lock:
            global generated_questions_count
            if generated_questions_count >= MAX_QUESTIONS:
                return []  # Early exit if the limit has been reached

            existing_questions = questions_grouped_by_topic[topic]
            allowed_count = min(NUMBER_OF_QUESTIONS_TO_GENERATE,
                                MAX_QUESTIONS - generated_questions_count)
            questions = generator_instance.generate_Q_turing_approach(
                PROMPT_PATH, topic, 3, existing_questions)

            new_problems = [{
                "metadata": {
                    "topic": topic,
                    "type": "query",
                    "difficulty": "Easy",
                    "target_length": 1
                },
                "messages": [{"role": "user", "content": question}]
            } for question in questions["questions"][:allowed_count]]

            generated_questions_count += len(new_problems)
        return new_problems

    # Run the generation process with multithreading
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        progress_bar = tqdm(total=MAX_QUESTIONS)
        futures = [executor.submit(generate_for_topic, topic)
                   for topic in questions_grouped_by_topic]

        for future in as_completed(futures):
            new_problems = future.result()
            problems.extend(new_problems)
            with generated_questions_lock:  # Make sure to synchronize access to the count
                progress_bar.update(len(new_problems))
        progress_bar.close()

    return problems


def run():
    '''
    Runs the question generation pipeline
    '''
    generator_instance = QuestionGenerator()
    questions_grouped_by_topics = get_existing_problems(QUESTIONS_DIR)
    all_topics = get_topics(TOPIC_PATH)
    print(all_topics)

    config = load_config(QGEN_CONFIG_PATH)

    new_problems = generate_questions_in_parallel(questions_grouped_by_topics,
        all_topics, config, generator_instance)
    print(new_problems)
    problem_titles, file_path_to_problem, problem_topic_counts = process_problems(
        new_problems)

    complexity_data = load_complexity_data(TOPIC_DIST_PATH)
    complexity_scores = flatten_complexity_scores(complexity_data)
    topic_percentiles = sort_and_rank_complexity_scores(complexity_scores)

    next_batch_id = get_next_batch_subdir(QUESTIONS_DIR)
    next_batch_path = os.path.join(QUESTIONS_DIR, next_batch_id)
    print(f"Got the next batch path: {next_batch_path}")
    create_and_save_notebook(next_batch_path.rstrip(
        '/') + "/{title}.ipynb", new_problems, problem_titles, topic_percentiles)
    process_notebooks_in_folder(next_batch_path)

    file_path_to_url = build_service_and_upload_files(
        SERVICE_ACCOUNT_FILE, next_batch_path, GDRIVE_URL_ME)
    update_problems_with_metadata(
        file_path_to_url, file_path_to_problem, next_batch_id)

    update_google_spreadsheet(SERVICE_ACCOUNT_FILE,
                              SPREADSHEET_ID_ME, new_problems)


if __name__ == '__main__':
    run()
