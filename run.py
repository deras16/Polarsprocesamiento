from src.main import main

if __name__ == '__main__' :   
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        raise e