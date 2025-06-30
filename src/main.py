from producer import GenerateVariants

def main():
    print("Hello from config-testerv2!")
    producer = GenerateVariants('varaints.db', "reference/output.json", seed=42)
    producer.start()
    
if __name__ == "__main__":
    main()