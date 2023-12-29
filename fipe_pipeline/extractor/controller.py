from fipe_pipeline.extractor.entity import FipeExtractor
from json.decoder import JSONDecodeError
import pandas as pd
import time


class ExtractorController:
    def __init__(self):
        self.extractor = FipeExtractor()

    def run(self):
        retry = True
        content = []
        try:
            data = pd.read_csv("data/fipe_data.csv")
            last_record = self.extractor._get_last_content(data)
        except pd.errors.EmptyDataError:
            data = pd.DataFrame()
            last_record = None
        print(last_record)
        try:
            retry = False
            dates: list = self.extractor._extract_dates()
            dates = [date["Mes"] for date in dates]
            print(dates)
            print(dates)
            for date in dates:
                brands = self.extractor._extract_brands(date)
                for brand in brands:
                    models = self.extractor._extract_models(date, brand)
                    for model in models:
                        retry = False
                        year_models = self.extractor._extract_year_models(
                            date, brand, model
                        )
                        for year_model in year_models:
                            retry = False
                            price_data = self.extractor._extract_full_price_data(
                                date, brand, model, year_model
                            )
                            json_data = {
                                **price_data,
                            }
                            content.append(json_data)
        except KeyboardInterrupt:
            print("KeyboardInterrupt")
            pd.DataFrame(content).to_csv("data/fipe_data.csv", index=False, mode="w+")
        except Exception as e:
            print(e)
            pd.DataFrame(content).to_csv("data/fipe_data.csv", index=False, mode="w+")
