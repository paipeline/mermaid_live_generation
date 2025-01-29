from typing import Optional
import openai
import os
import dotenv
from kafka import KafkaProducer
import json
import datetime

dotenv.load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'mermaid_graphs'

class MermaidGraphGenerator:
    def __init__(self):
        """Initialize Mermaid graph generator and Kafka producer"""
        openai.api_key = OPENAI_API_KEY
        self.producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def generate_mermaid_code(self, prompt: str) -> Optional[str]:
        """Generate Mermaid code based on user prompt
        
        Args:
            prompt (str): User's description of desired diagram
            
        Returns:
            str: Generated Mermaid code
        """
        try:
            messages = [
                {"role": "system", "content": "You are a professional Mermaid diagram expert. Please generate Mermaid code based on the user's description. Return only the code, no additional explanation."},
                {"role": "user", "content": f"Please generate Mermaid diagram code for the following description: {prompt}"}
            ]
            
            client = openai.OpenAI()
            response = client.chat.completions.create(
                model="gpt-4",
                messages=messages,
                temperature=0.7,
            )
            
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"Error generating Mermaid code: {str(e)}")
            return None

    def validate_mermaid_code(self, mermaid_code: str) -> bool:
        """Validate if the generated Mermaid code is valid
        
        Args:
            mermaid_code (str): Mermaid code to validate
            
        Returns:
            bool: Whether the code is valid
        """
        basic_keywords = [
            'graph', 'flowchart', 'sequenceDiagram', 
            'classDiagram', 'stateDiagram', 'gantt',
            'pie', 'mindmap', 'timeline'
        ]
        return any(keyword in mermaid_code for keyword in basic_keywords)

    def process_user_request(self, prompt: str) -> Optional[str]:
        """Process user request and send to Kafka"""
        mermaid_code = self.generate_mermaid_code(prompt)
        
        if not mermaid_code:
            return None
            
        if self.validate_mermaid_code(mermaid_code):
            # Send to Kafka
            message = {
                'prompt': prompt,
                'mermaid_code': mermaid_code,
                'timestamp': str(datetime.datetime.now())
            }
            self.producer.send(KAFKA_TOPIC, value=message)
            self.producer.flush()
            return mermaid_code
        else:
            print("Generated Mermaid code is invalid")
            return None


if __name__ == "__main__":
    generator = MermaidGraphGenerator()
    prompt = "Please generate a simple flowchart describing the process from user login to system logout."
    mermaid_code = generator.process_user_request(prompt)
    print(mermaid_code)