AOF_SHEET_ID = "1bsM3nz13BPPqCxmbLYFz5Ed9KOfV51hNoagBP8dP948"
AOF_SHEET_NAME = "Areas status"
import os
from typing import List

from dotenv import find_dotenv, load_dotenv
from llama_index.llms.openai import OpenAI
from llama_index.program import OpenAIPydanticProgram
from pydantic import BaseModel, Field
from tqdm import tqdm
# from utils import PROJECT_ROOT

# from src.sheets_utils import download_sheet_as_df

api_key = os.environ["OPENAI_API_KEY"]

load_dotenv(find_dotenv())
from llama_index.llms.openai import OpenAI
from pydantic import BaseModel
from py_files.utils import process_batch, service_account_path


def format_topics_with_additional(df):
    topics = []
    last_highlevel = None
    custom_sub_cat = "[[fill out another topic you think best fits the conversation and is not included, 3 words max]]"
    for _, row in df.iterrows():
        highlevel = row["Highlevel_topic"]
        sublevel = row["Sublevel_topic"]
        if highlevel != last_highlevel and last_highlevel is not None:
            topics.append(f"{last_highlevel} -> {custom_sub_cat}")

        topics.append(f"{highlevel} -> {sublevel.strip(' - ')}")
        # Add another entry with the same highlevel topic but a placeholder for a new sublevel topic
        # only if the highlevel topic has changed from the last one processed
        last_highlevel = highlevel
    topics.append(f"{last_highlevel} -> {custom_sub_cat}")
    topics.append(f"Other -> {custom_sub_cat}")
    return "\n".join(topics)


import json


class DomainCategory(BaseModel):
    """Datamodel for the category and subcategory calssification."""
    top_level: str
    # sub_level: str
    # detailed_level: str
    # new_filled_category: bool


def classify_conversation_by_action(
    conversation: List[dict], domain_categories
) -> DomainCategory:
    prompt_template_str = """
    Categorize the theme of user requests in the following conversation by domain into one of the following top-level categories.
    
    Categories:
    {categories}

    Conversation:
    {conversation}

    """

    program = OpenAIPydanticProgram.from_defaults(
        llm=OpenAI(api_key=api_key, model="gpt-4-1106-preview", temperature=0),
        output_cls=DomainCategory,
        prompt_template_str=prompt_template_str,
        verbose=False,
    )
    output = program(
        categories=domain_categories,
        conversation=conversation["messages"],
    )
    return output


import concurrent.futures


def process_file(conversation, domain_categories):
    output = classify_conversation_by_action(conversation, domain_categories)
    record = {
        "id": conversation["id"],
        "colab_link": f"https://colab.research.google.com/drive/{conversation['id']}",
    }
    record.update(output.dict())
    return record


def get_areas_of_focus_data_batch_conversations(
    selected_batch_folder, max_workers=10, limit_items_to_first_n=None
):
    DOMAIN_CATEGORIES = """
    - Write code in python
    - Explain code
    - Fix / refactor / optimize code
    - Debug error trace
    - Write unit tests
    - Write CI/CD code
    - Do a code review
    - Write / modify / fix beam code
    - Write / modify / fix spark code
    - Write end to end ML training code
    - Help me take an interview
    - Answer ML research questions
    - Answer infra questions
    - Write / modify / fix SQL code
    - Write / modify / fix JavaScript code
    - Scrape a website
    """
    selected_conversations = process_batch(
        selected_batch_folder, limit=limit_items_to_first_n
    )
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_file, conversation, DOMAIN_CATEGORIES)
            for conversation in selected_conversations
        ]
        progress_bar = tqdm(total=len(futures), desc="Processing Conversations")
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
            progress_bar.update(1)
        progress_bar.close()

    return results
