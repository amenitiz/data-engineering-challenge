from model import Model
from map import Converter


def main():
    Model().create_csv()
    Converter().save_png()


if __name__ == "__main__":
    main()


