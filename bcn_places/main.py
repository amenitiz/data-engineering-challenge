import model
import map

model = model.Model()
output_places = map.Converter()


def main():
    model.create_csv()
    output_places.save_png()


if __name__ == "__main__":
    main()








