import unittest

from src.Q_generator.question_processor import *


# Test Question Processor

class TestQuestionProcessor(unittest.TestCase):

    def test_load_problems(self):
        problems = load_problems("data/problems.json")
        self.assertTrue(len(problems) > 0)

    def test_extract_questions(self):
        problems = load_problems("data/problems.json")
        questions = extract_questions(problems)
        self.assertTrue(len(questions) > 0)

    def test_find_and_load_all_problems(self):
        problems = find_and_load_all_problems("data")
        self.assertTrue(len(problems) > 0)

    def test_extract_questions_by_topic(self):
        problems = load_problems("data/problems.json")
        questions_by_topic = extract_questions_by_topic(problems)
        self.assertTrue(len(questions_by_topic) > 0)

    def test_process_problems(self):
        problems = load_problems("data/problems.json")
        problem_titles, file_path_to_problem, problem_topic_counts = process_problems(problems)
        self.assertTrue(len(problem_titles) > 0)
        self.assertTrue(len(file_path_to_problem) > 0)
        self.assertTrue(len(problem_topic_counts) > 0)

    def test_load_complexity_data(self):
        complexity_data = load_complexity_data("data/complexity_data.json")
        self.assertTrue(len(complexity_data) > 0)

    def test_flatten_complexity_scores(self):
        complexity_data = load_complexity_data("data/complexity_data.json")
        scores_dict = flatten_complexity_scores(complexity_data)
        self.assertTrue(len(scores_dict) > 0)

    def test_sort_and_rank_complexity_scores(self):
        complexity_data = load_complexity_data("data/complexity_data.json")
        sorted_scores = sort_and_rank_complexity_scores(complexity_data)
        self.assertTrue(len(sorted_scores) > 0)

    def test_get_turn_range(self):
        turns = 5
        min_turns, max_turns = get_turn_range(turns)
        self.assertTrue(min_turns <= turns <= max_turns)

    def test_biased_normal_distribution(self):
        input_number = 50
        min_val = 1
        max_val = 15
        std_dev = 2.5
        base_mean = 1
        sample = biased_normal_distribution(input_number, min_val, max_val, std_dev, base_mean)
        self.assertTrue(min_val <= sample <= max_val)

    def test_create_and_save_notebook(self):
        notebook_path = "data/test.ipynb"
        problems = load_problems("data/problems.json")
        problem_titles = ["test1", "test2"]
        topic_percentiles = {"test1": 0.5, "test2": 0.75}
        create_and_save_notebook(notebook_path, problems, problem_titles, topic_percentiles)
        self.assertTrue(os.path.exists(notebook_path))

    def test_crawl_keys(self):
        d = {"a": 1, "b": {"c": 2, "d": 3}}
        paths = crawl_keys(d)
        self.assertTrue(len(paths) > 0)

    def test_extract_metadata_from_cell(self):
        cell_content = """# Metadata

**Occupation Topics** - Software Engineering > Machine Learning - Data Science

**Target Number of Turns (User + Assistant)** - 5

**Use Case** - Learning

**Technical Topic** - Machine Learning

**User Personality** - Curious"""
        metadata = extract_metadata_from_cell(cell_content)
        self.assertTrue(len(metadata) > 0)

    def test_parse_occupation_data(self):
        occupation_data = "First Timer - Software Engineering > Machine Learning - Data Science"
        experience_level, learning_topic = parse_occupation_data(occupation_data)
        self.assertTrue(experience_level == "First Timer")
        self.assertTrue(learning_topic == "Machine Learning - Data Science")

    def test_clean_markdown_cell(self):
        cell_source = """# Metadata

**Occupation Topics** - Software Engineering > Machine Learning - Data Science

**Target Number of Turns (User + Assistant)** - 5

**Use Case** - Learning

**Technical Topic** - Machine Learning

**User Personality** - Curious"""
        metadata = {"occupation": "Software Engineering > Machine Learning - Data Science", "experience_level": "First Timer", "learning_topic": "Machine Learning - Data Science", "use_case": "Learning", "technical_topic": "Machine Learning", "personality": "Curious"}
        cleaned_cell_source = clean_markdown_cell(cell_source, metadata)
        self.assertTrue("Unknown" not in cleaned_cell_source)

    def test_modify_notebook(self):
        notebook_path = "data/test.ipynb"
        modify_notebook(notebook_path)
        self.assertTrue(os.path.exists(notebook_path))

    def test_process_notebooks_in_folder(self):
        folder_path = "data"
        process_notebooks_in_folder(folder_path)
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()
