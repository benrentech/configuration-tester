from producer import GenerateVariants

def main():
    print("Hello from config-testerv2!")
    producer = GenerateVariants("reference/output.json", seed=42)
    producer.generate()
    
if __name__ == "__main__":
    main()