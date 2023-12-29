import requests
from pandas import DataFrame


class FipeExtractor:
    def __init__(self):
        self.base_url = "http://veiculos.fipe.org.br/api/veiculos/"
        self.headers = {
            "Host": "veiculos.fipe.org.br",
            "Referer": "http://veiculos.fipe.org.br/",
        }

    def _extract_dates(self):
        return requests.post(
            "http://veiculos.fipe.org.br/api/veiculos/ConsultarTabelaDeReferencia",
            headers=self.headers,
        ).json()

    def _extract_brands(self, date: str):
        return requests.post(
            "https://veiculos.fipe.org.br/api/veiculos/ConsultarMarcas",
            headers=self.headers,
            json={"codigoTabelaReferencia": date["Codigo"], "codigoTipoVeiculo": 1},
        ).json()

    def _extract_models(self, date: str, brand: str):
        return (
            requests.post(
                "https://veiculos.fipe.org.br/api/veiculos/ConsultarModelos",
                headers={
                    "Host": "veiculos.fipe.org.br",
                    "Referer": "http://veiculos.fipe.org.br/",
                },
                json={
                    "codigoTabelaReferencia": date["Codigo"],
                    "codigoTipoVeiculo": 1,
                    "codigoMarca": brand["Value"],
                },
            )
            .json()
            .get("Modelos")
        )

    def _extract_year_models(self, date: str, brand: str, model: str):
        return requests.post(
            "https://veiculos.fipe.org.br/api/veiculos/ConsultarAnoModelo",
            headers={
                "Host": "veiculos.fipe.org.br",
                "Referer": "http://veiculos.fipe.org.br/",
            },
            json={
                "codigoTabelaReferencia": date["Codigo"],
                "codigoTipoVeiculo": 1,
                "codigoMarca": brand["Value"],
                "codigoModelo": model["Value"],
            },
        ).json()

    def _extract_full_price_data(self, date: str, brand: str, model: str, year: str):
        return requests.post(
            "https://veiculos.fipe.org.br/api/veiculos/ConsultarValorComTodosParametros",
            headers={
                "Host": "veiculos.fipe.org.br",
                "Referer": "http://veiculos.fipe.org.br/",
            },
            json={
                "codigoTabelaReferencia": date["Codigo"],
                "codigoTipoVeiculo": 1,
                "codigoMarca": brand["Value"],
                "codigoModelo": model["Value"],
                "anoModelo": year["Value"].split("-")[0],
                "codigoTipoCombustivel": year["Value"].split("-")[1],
                "tipoVeiculo": 1,
                "modeloCodigoExterno": "",
                "tipoConsulta": "tradicional",
            },
        ).json()

    def _get_last_content(self, dataframe: DataFrame):
        return dataframe.tail(1).to_dict("records")[0]
