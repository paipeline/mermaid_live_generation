from kafka import KafkaConsumer
import json
import os
import subprocess
import tempfile
from datetime import datetime
import hashlib

class MermaidGraphConsumer:
    def __init__(self):
        """Initialize Kafka consumer"""
        self.consumer = KafkaConsumer(
            'mermaid_graphs',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        
        # 确保图片保存目录存在
        self.img_dir = 'img'
        os.makedirs(self.img_dir, exist_ok=True)

    def generate_filename(self, mermaid_code: str) -> str:
        """Generate unique filename based on Mermaid code
        
        Args:
            mermaid_code (str): Mermaid code
            
        Returns:
            str: Generated filename
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        code_hash = hashlib.md5(mermaid_code.encode()).hexdigest()[:8]
        return f"mermaid_{timestamp}_{code_hash}.png"

    def save_mermaid_to_image(self, mermaid_code: str, output_path: str) -> bool:
        """Convert Mermaid code to image
        
        Args:
            mermaid_code (str): Mermaid code
            output_path (str): Output image path
            
        Returns:
            bool: Whether image generation was successful
        """
        try:
            # Create temporary file to store Mermaid code
            with tempfile.NamedTemporaryFile(mode='w', suffix='.mmd', delete=False) as temp_file:
                # Ensure code doesn't contain ```mermaid markers
                clean_code = mermaid_code.replace('```mermaid', '').replace('```', '').strip()
                temp_file.write(clean_code)
                temp_file_path = temp_file.name

            # 使用 mmdc (mermaid-cli) 生成图片
            subprocess.run([
                'mmdc',
                '-i', temp_file_path,
                '-o', output_path,
                '-b', 'transparent'
            ], check=True)

            # 删除临时文件
            os.unlink(temp_file_path)
            return True
        except Exception as e:
            print(f"Error generating image: {str(e)}")
            return False

    def start_consuming(self):
        """Start consuming Kafka messages and generate images"""
        print("Starting to listen for Mermaid graph messages...")
        
        for message in self.consumer:
            try:
                data = message.value
                mermaid_code = data['mermaid_code']
                
                # 生成输出文件路径
                output_filename = self.generate_filename(mermaid_code)
                output_path = os.path.join(self.img_dir, output_filename)
                
                # 生成图片
                if self.save_mermaid_to_image(mermaid_code, output_path):
                    print(f"Successfully generated image: {output_path}")
                else:
                    print("Failed to generate image")
                    
            except Exception as e:
                print(f"Error processing message: {str(e)}")

if __name__ == "__main__":
    consumer = MermaidGraphConsumer()
    consumer.start_consuming()
