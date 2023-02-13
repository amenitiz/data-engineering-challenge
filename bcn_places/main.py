from model import Model
from map import Converter

model = Model()
output_places = Converter()


def main():
    model.create_csv()
    output_places.save_png()


if __name__ == "__main__":
    main()








