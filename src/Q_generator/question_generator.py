import json
import os
from typing import List, Optional
from openai import OpenAI

class QuestionGenerator:
    def __init__(self):
        self.openai_api_key = os.environ.get("OPENAI_API_KEY")
        self.client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    
    def _generate_questions(self, system_prompt, topic, n, existing_questions, response_format):
        try:
            response = self.client.chat.completions.create(
                model="gpt-4-1106-preview",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Topic: {topic} \nNumber of questions: {n} \n existing questions {existing_questions}"},
                ],
                temperature=0.0,
                max_tokens=4096,
                seed=42,
                response_format=response_format,
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def generate_Q_turing_approach(self, prompt_path: str, topic: str, n: int = 100, existing_questions: Optional[List[str]] = None) -> Optional[List[str]]:
        
        """
        Generate a set of human-like questions on a specific Python topic with constraints to ensure uniqueness and diversity.

        Parameters:
        topic: str
            The topic for which the questions are to be generated.
        n: int, optional
            The number of questions to generate. Default is 100.
        existing_questions: List[str], optional
            Previously generated questions that should not be duplicated. Default is None.

        Returns:
        List[str] or None
            A JSON-valid list of generated questions if successful, None otherwise.

        Raises:
        Any exception triggered during the communication with GPT-4 or JSON processing is printed to the console.
        """
        with open(prompt_path, 'r') as file:
            system_prompt = file.read()

        return self._generate_questions(system_prompt, topic, n, existing_questions, {"type": "json_object"})

    def compatible_area_check(self, prompt_path, occupation_skill, areas_of_focus, existing_questions=None):
        with open(prompt_path, 'r') as file:
            system_prompt = file.read()

        try:
            response = self.client.chat.completions.create(
                model="gpt-4-1106-preview",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Find the compatible areas of focus for the provided occupation and skill pair."}
                ],
                temperature=0.0,
                max_tokens=4096,
                stop=None,
                seed=42
            )

            questions = json.loads(response.choices[0].message.content) if response.choices[0].message.content else {}
            return questions

        except Exception as e:
            print(f"An error occurred: {e}")
            return None
        

    def generate_usecase_technical_topic(self, prompt_path, occupation, n=1, existing_questions=None):

        with open(prompt_path, 'r') as file:
            system_prompt = file.read()
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4-1106-preview",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"occupation_experience level and compatible area of focus: {occupation} \nNumber of questions: {n} \n existing questions {existing_questions}"},
                ],
                temperature=0.0,
                max_tokens=4096,
                seed = 42,
                response_format={ 
                    "type": "json_object" 
                },
            )
            questions = json.loads(response.choices[0].message.content)
            return questions
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
