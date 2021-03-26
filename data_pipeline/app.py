from pipeline import load_data


def main():
    data_path = "../data/techcrunch.csv"
    result = load_data(data_path)
    print("Result: ", result)


if __name__ == "__main__":
    main()