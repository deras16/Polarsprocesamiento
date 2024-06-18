from src.main import main

if __name__ == '__main__' :   
    """
    This is the entry point of the application
    """
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        raise e
