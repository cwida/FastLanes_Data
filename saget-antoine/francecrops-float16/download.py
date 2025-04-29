import json
from datasets import load_dataset

def serialize_nested(example):
    example["x"] = json.dumps(example["x"])
    return example

def main():
    dataset = load_dataset("saget-antoine/francecrops-float16")
    train_dataset = dataset["train"]

    # Select only first 1 row for testing
    small_dataset = train_dataset.select(range(1))

    # Serialize the nested 'x' field
    small_dataset = small_dataset.map(serialize_nested)

    # Convert to pandas
    df = small_dataset.to_pandas()

    # Save to CSV
    df.to_csv("francecrops_train_sample.csv", index=False)

if __name__ == "__main__":
    main()
