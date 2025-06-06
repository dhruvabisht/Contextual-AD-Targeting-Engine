from datasets import load_dataset
import os

# Create data directory if it doesn't exist
os.makedirs('data', exist_ok=True)

# Load AG News dataset
dataset = load_dataset("ag_news", split="train[:200]")

# Write to training.txt
with open("data/training.txt", "w") as f:
    for item in dataset:
        label = ["World", "Sports", "Business", "Sci/Tech"][item['label']]
        f.write(f"__label__{label}  {item['text'].strip()}\n")

print("Training data has been generated in data/training.txt") 