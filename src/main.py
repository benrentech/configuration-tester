from producer import GenerateVariants
from consumer import SendRequests

def main():
    producer = GenerateVariants("reference/output.json", "variants.db", seed=42)
    producer.generate()

    consumer = SendRequests("variants.db", "https://httpbin.org/post")
    consumer.start_async(num_workers=30)
    
if __name__ == "__main__":
    main()