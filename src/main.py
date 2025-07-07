from producer import GenerateVariants
from consumer import Sender

def main():
    producer = GenerateVariants("reference/output.json", "variants.db", seed=42)
    producer.generate()

    sender = Sender("variants.db", "https://httpbin.org/post")
    sender.run(num_workers=100)
    
if __name__ == "__main__":
    main()