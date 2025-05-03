from datetime import date

from build_merc_table import add_inflation_columns
from build_merc_table import add_prev_original_price
from build_merc_table import cast_price_columns_as_double
from build_merc_table import deduplicate_products_with_diff_prices_per_date
from build_merc_table import INGESTION_SCHEMA
from build_merc_table import map_old_categories
from build_merc_table import process_raw_data
from build_merc_table import RESULTING_SCHEMA
from build_merc_table import split_category_subcategory
from build_merc_table import standardize_string_columns
from pyspark.sql.types import DateType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from tests.conf_test import SparkTestCase


class TestIntegrationMercadona(SparkTestCase):
    def test_process_raw_data(self):
        test_data = [
            ("Aceite", "6,75 €", None, "Botella 1 L", "Aceite > Aceite", date(2024, 11, 20)),
            ("Aceite", "7,75 €", None, "Botella 1 L", "Aceite > Aceite", date(2024, 11, 21)),
            ("Aceite", "8,75 €", None, "Botella 1 L", "Aceite > Aceite", date(2024, 11, 22)),
            ("Monster", "1,79 €", "1,45 €", "Lata 500 ml", "Refrescos > Isotónico", date(2024, 11, 20)),
            ("Monster", "1,85 €", "1,79 €", "Lata 500 ml", "Refrescos > Isotónico", date(2024, 11, 21)),
            ("Monster", "1,85 €", "1,79 €", "Lata 500 ml", "Refrescos > Isotónico", date(2024, 11, 22)),
        ]
        test_df = self.spark.createDataFrame(test_data, INGESTION_SCHEMA)

        expected_data = [
            (date(2024, 11, 20), 1, "aceite", "botella 1 l", "aceite", "aceite", 6.75, None, None, None, None, None),
            (
                date(2024, 11, 21),
                1,
                "aceite",
                "botella 1 l",
                "aceite",
                "aceite",
                7.75,
                6.75,
                None,
                None,
                (7.75 / 6.75) - 1,
                1.0,
            ),
            (
                date(2024, 11, 22),
                1,
                "aceite",
                "botella 1 l",
                "aceite",
                "aceite",
                8.75,
                7.75,
                None,
                None,
                (8.75 / 7.75) - 1,
                1.0,
            ),
            (
                date(2024, 11, 20),
                1,
                "monster",
                "lata 500 ml",
                "refrescos",
                "isotonico",
                1.79,
                None,
                1.45,
                None,
                None,
                None,
            ),
            (
                date(2024, 11, 21),
                1,
                "monster",
                "lata 500 ml",
                "refrescos",
                "isotonico",
                1.85,
                1.79,
                1.79,
                True,
                (1.85 / 1.79) - 1,
                1.85 - 1.79,
            ),
            (
                date(2024, 11, 22),
                1,
                "monster",
                "lata 500 ml",
                "refrescos",
                "isotonico",
                1.85,
                1.85,
                1.79,
                None,
                0.00,
                0.00,
            ),
        ]
        expected_df = self.spark.createDataFrame(expected_data, RESULTING_SCHEMA)

        actual_df = process_raw_data(test_df)
        self.assert_spark_dataframes_equal(actual_df, expected_df)


class TestMercadona(SparkTestCase):
    def test_cast_price_columns_as_float(self):
        price_columns = ["original_price", "discount_price"]
        test_data = [
            ("0,33 €", None),
            ("2,24 €", "2,15 €"),
            ("2,55 €", None),
            ("5.99", None),
            ("€", None),
            ("", None),
            (None, None),
        ]
        test_df = self.spark.createDataFrame(test_data, price_columns)
        expected_data = [
            (0.33, None),
            (2.24, 2.15),
            (2.55, None),
            (5.99, None),
            (None, None),
            (None, None),
            (None, None),
        ]
        expected_df = self.spark.createDataFrame(expected_data, price_columns)
        actual_df = cast_price_columns_as_double(test_df)
        self.assert_spark_dataframes_equal(expected_df, actual_df)

    def test_split_category_subcategory(self):
        test_data = [
            ("Agua y refrescos > Refresco de cola",),
            ("Agua y refrescos > Refresco de té y sin gas",),
            ("Aperitivos > Frutos secos y fruta desecada",),
            ("home",),
            (None,),
        ]
        test_df = self.spark.createDataFrame(test_data, ["category"])
        expected_data = [
            ("Agua y refrescos", "Refresco de cola"),
            ("Agua y refrescos", "Refresco de té y sin gas"),
            ("Aperitivos", "Frutos secos y fruta desecada"),
            ("home", None),
            (None, None),
        ]
        expected_df = self.spark.createDataFrame(expected_data, ["category", "subcategory"])
        actual_df = split_category_subcategory(test_df)
        self.assert_spark_dataframes_equal(expected_df, actual_df)

    def test_standardize_string_columns(self):
        string_columns = ["name", "size", "category", "subcategory"]
        test_data = [
            (
                "Pechuga de pavo bajo en sal Hacendado finas lonchas",
                "2 paquetes x 200 g",
                "Charcutería y quesos",
                "Aves y jamón cocido",
            ),
            (
                "Edulcorante en pastillas sacarina Hacendado",
                "Bote 850 pastillas (52 g)",
                "Azúcar, caramelos y chocolate",
                "Azúcar y edulcorante",
            ),
            ("Cerveza 0,0% sin alcohol Falke", "6 botellines x 250 ml", "Bodega", "Cerveza sin alcohol"),
        ]
        test_df = self.spark.createDataFrame(test_data, string_columns)
        expected_data = [
            (
                "pechuga de pavo bajo en sal hacendado finas lonchas",
                "2 paquetes x 200 g",
                "charcuteria y quesos",
                "aves y jamon cocido",
            ),
            (
                "edulcorante en pastillas sacarina hacendado",
                "bote 850 pastillas (52 g)",
                "azucar, caramelos y chocolate",
                "azucar y edulcorante",
            ),
            ("cerveza 0,0% sin alcohol falke", "6 botellines x 250 ml", "bodega", "cerveza sin alcohol"),
        ]
        expected_df = self.spark.createDataFrame(expected_data, string_columns)
        actual_df = standardize_string_columns(test_df)
        self.assert_spark_dataframes_equal(expected_df, actual_df)

    def test_map_old_categories(self):
        # Join columns: name and size
        test_schema = StructType(
            [
                StructField("date", DateType()),
                StructField("name", StringType()),
                StructField("size", StringType()),
                StructField("category", StringType()),
                StructField("subcategory", StringType()),
            ]
        )
        test_data = [
            # Old records with old categories
            (
                date(2024, 11, 5),
                "bebida energetica energy drink zero hacendado",
                "6 latas x 250 ml",
                "sport-drinks",
                None,
            ),
            (
                date(2024, 11, 5),
                "refresco de limon hacendado fresh gas",
                "lata 330 ml",
                "soft-drinks",
                None,
            ),
            (
                date(2024, 11, 5),
                "aceitunas verdes rellenas de anchoa la alcoyana",
                "bote 830 g 350 g escurrido",
                "appetizer",
                None,
            ),
            # New records with new categories
            (
                date(2024, 11, 10),
                "bebida energetica energy drink zero hacendado",
                "6 latas x 250 ml",
                "agua y refrescos",
                "isotonico y energetico",
            ),
            (
                date(2024, 11, 10),
                "refresco de limon hacendado fresh gas",
                "lata 330 ml",
                "agua y refrescos",
                "refresco de naranja y de limon",
            ),
            (
                date(2024, 11, 10),
                "aceitunas verdes rellenas de anchoa la alcoyana",
                "bote 830 g 350 g escurrido",
                "aperitivos",
                "aceitunas y encurtidos",
            ),
        ]
        test_df = self.spark.createDataFrame(test_data, test_schema)

        expected_data = [
            # Old records with new corrected categories
            (
                date(2024, 11, 5),
                "bebida energetica energy drink zero hacendado",
                "6 latas x 250 ml",
                "agua y refrescos",
                "isotonico y energetico",
            ),
            (
                date(2024, 11, 5),
                "refresco de limon hacendado fresh gas",
                "lata 330 ml",
                "agua y refrescos",
                "refresco de naranja y de limon",
            ),
            (
                date(2024, 11, 5),
                "aceitunas verdes rellenas de anchoa la alcoyana",
                "bote 830 g 350 g escurrido",
                "aperitivos",
                "aceitunas y encurtidos",
            ),
            # New records with new categories
            (
                date(2024, 11, 10),
                "bebida energetica energy drink zero hacendado",
                "6 latas x 250 ml",
                "agua y refrescos",
                "isotonico y energetico",
            ),
            (
                date(2024, 11, 10),
                "refresco de limon hacendado fresh gas",
                "lata 330 ml",
                "agua y refrescos",
                "refresco de naranja y de limon",
            ),
            (
                date(2024, 11, 10),
                "aceitunas verdes rellenas de anchoa la alcoyana",
                "bote 830 g 350 g escurrido",
                "aperitivos",
                "aceitunas y encurtidos",
            ),
        ]
        expected_df = self.spark.createDataFrame(expected_data, test_schema)

        actual_df = map_old_categories(test_df)
        self.assert_spark_dataframes_equal(expected_df, actual_df)

    def test_deduplicate_products_with_diff_prices_per_date(self):
        test_schema = StructType(
            [
                StructField("date", DateType()),
                StructField("name", StringType()),
                StructField("size", StringType()),
                StructField("category", StringType()),
                StructField("subcategory", StringType()),
                StructField("original_price", DoubleType()),
            ]
        )
        test_data = [
            # Same product but different prices on 2024-11-03
            (
                date(2024, 11, 3),
                "estropajo limpieza delicada bosque verde",
                "paquete 3 ud.",
                "limpieza y hogar",
                "estropajo, bayeta y guantes",
                0.75,
            ),
            (
                date(2024, 11, 3),
                "estropajo limpieza delicada bosque verde",
                "paquete 3 ud.",
                "limpieza y hogar",
                "estropajo, bayeta y guantes",
                0.9,
            ),
            # Same product but different prices on 2024-11-04
            (
                date(2024, 11, 4),
                "estropajo limpieza delicada bosque verde",
                "paquete 3 ud.",
                "limpieza y hogar",
                "estropajo, bayeta y guantes",
                0.75,
            ),
            (
                date(2024, 11, 4),
                "estropajo limpieza delicada bosque verde",
                "paquete 3 ud.",
                "limpieza y hogar",
                "estropajo, bayeta y guantes",
                0.9,
            ),
            (
                date(2024, 11, 4),
                "bebida energetica energy drink zero hacendado",
                "6 latas x 250 ml",
                "agua y refrescos",
                "isotonico y energetico",
                2.99,
            ),
            # Just 1 product on 2024-11-05
            (
                date(2024, 11, 5),
                "estropajo limpieza delicada bosque verde",
                "paquete 3 ud.",
                "limpieza y hogar",
                "estropajo, bayeta y guantes",
                0.9,
            ),
        ]
        test_df = self.spark.createDataFrame(test_data, test_schema)

        expected_schema = StructType(
            [
                StructField("date", DateType()),
                StructField("name", StringType()),
                StructField("size", StringType()),
                StructField("category", StringType()),
                StructField("subcategory", StringType()),
                StructField("original_price", DoubleType()),
                StructField("dedup_id", IntegerType(), nullable=False),
            ]
        )
        expected_data = [
            (
                date(2024, 11, 3),
                "estropajo limpieza delicada bosque verde",
                "paquete 3 ud.",
                "limpieza y hogar",
                "estropajo, bayeta y guantes",
                0.75,
                1,
            ),
            (
                date(2024, 11, 3),
                "estropajo limpieza delicada bosque verde",
                "paquete 3 ud.",
                "limpieza y hogar",
                "estropajo, bayeta y guantes",
                0.9,
                2,
            ),
            # Same product but different prices on 2024-11-04
            (
                date(2024, 11, 4),
                "estropajo limpieza delicada bosque verde",
                "paquete 3 ud.",
                "limpieza y hogar",
                "estropajo, bayeta y guantes",
                0.75,
                1,
            ),
            (
                date(2024, 11, 4),
                "estropajo limpieza delicada bosque verde",
                "paquete 3 ud.",
                "limpieza y hogar",
                "estropajo, bayeta y guantes",
                0.9,
                2,
            ),
            (
                date(2024, 11, 4),
                "bebida energetica energy drink zero hacendado",
                "6 latas x 250 ml",
                "agua y refrescos",
                "isotonico y energetico",
                2.99,
                1,
            ),
            # Just 1 product on 2024-11-05
            (
                date(2024, 11, 5),
                "estropajo limpieza delicada bosque verde",
                "paquete 3 ud.",
                "limpieza y hogar",
                "estropajo, bayeta y guantes",
                0.9,
                1,
            ),
        ]
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        actual_df = deduplicate_products_with_diff_prices_per_date(test_df)
        self.assert_spark_dataframes_equal(expected_df, actual_df)

    def test_add_prev_original_price(self):
        test_schema = StructType(
            [
                StructField("date", DateType()),
                StructField("name", StringType()),
                StructField("size", StringType()),
                StructField("original_price", StringType()),
                StructField("discount_price", StringType()),
                StructField("dedup_id", IntegerType(), nullable=False),
            ]
        )
        test_data = [
            # Coke Zero
            (date(2024, 11, 3), "coke zero", "12 latas x 330 ml", 10.44, None, 1),
            (date(2024, 11, 4), "coke zero", "12 latas x 330 ml", 9.72, None, 1),
            (date(2024, 11, 8), "coke zero", "12 latas x 330 ml", 10.44, 9.72, 1),
            (date(2024, 11, 9), "coke zero", "12 latas x 330 ml", 10.44, 9.72, 1),
            # Sunny Juice
            (date(2024, 11, 5), "sunny delight", "botella 1,25 l", 1.5, None, 1),
            (date(2024, 11, 6), "sunny delight", "botella 1,25 l", 1.5, None, 1),
            (date(2024, 11, 7), "sunny delight", "botella 1,25 l", 1.5, None, 1),
            (date(2024, 11, 8), "sunny delight", "botella 1,25 l", 1.95, 1.5, 1),
            (date(2024, 11, 9), "sunny delight", "botella 1,25 l", 1.95, 1.5, 1),
            # Same products with different prices on 1 day
            (date(2024, 11, 3), "estropajo bosque verde", "paquete 3 ud.", 0.75, None, 1),
            (date(2024, 11, 3), "estropajo bosque verde", "paquete 3 ud.", 0.9, None, 2),
            (date(2024, 11, 4), "estropajo bosque verde", "paquete 3 ud.", 0.75, None, 1),
            (date(2024, 11, 4), "estropajo bosque verde", "paquete 3 ud.", 0.9, None, 2),
        ]
        test_df = self.spark.createDataFrame(test_data, test_schema)

        expected_schema = StructType(
            [
                StructField("date", DateType()),
                StructField("name", StringType()),
                StructField("size", StringType()),
                StructField("original_price", StringType()),
                StructField("discount_price", StringType()),
                StructField("dedup_id", IntegerType(), nullable=False),
                StructField("prev_original_price", StringType()),
            ]
        )
        expected_data = [
            # Coke Zero
            (date(2024, 11, 3), "coke zero", "12 latas x 330 ml", 10.44, None, 1, None),
            (date(2024, 11, 4), "coke zero", "12 latas x 330 ml", 9.72, None, 1, 10.44),
            (date(2024, 11, 8), "coke zero", "12 latas x 330 ml", 10.44, 9.72, 1, 9.72),
            (date(2024, 11, 9), "coke zero", "12 latas x 330 ml", 10.44, 9.72, 1, 10.44),
            # Sunny Juice
            (date(2024, 11, 5), "sunny delight", "botella 1,25 l", 1.5, None, 1, None),
            (date(2024, 11, 6), "sunny delight", "botella 1,25 l", 1.5, None, 1, 1.5),
            (date(2024, 11, 7), "sunny delight", "botella 1,25 l", 1.5, None, 1, 1.5),
            (date(2024, 11, 8), "sunny delight", "botella 1,25 l", 1.95, 1.5, 1, 1.5),
            (date(2024, 11, 9), "sunny delight", "botella 1,25 l", 1.95, 1.5, 1, 1.95),
            # Dup product 1
            (date(2024, 11, 3), "estropajo bosque verde", "paquete 3 ud.", 0.75, None, 1, None),
            (date(2024, 11, 4), "estropajo bosque verde", "paquete 3 ud.", 0.75, None, 1, 0.75),
            # Dup product 2
            (date(2024, 11, 3), "estropajo bosque verde", "paquete 3 ud.", 0.9, None, 2, None),
            (date(2024, 11, 4), "estropajo bosque verde", "paquete 3 ud.", 0.9, None, 2, 0.9),
        ]
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        actual_df = add_prev_original_price(test_df)
        self.assert_spark_dataframes_equal(expected_df, actual_df)

    def test_add_inflation_columns(self):
        test_columns = ["original_price", "prev_original_price"]
        test_data = [
            (1.0, None),
            (1.0, 1.0),
            (1.5, 1.0),
            (3.0, 1.5),
            (2.0, 3.0),
        ]
        test_df = self.spark.createDataFrame(test_data, test_columns)

        expected_columns = ["original_price", "prev_original_price", "inflation_percent", "inflation_abs"]
        expected_data = [
            (1.0, None, None, None),
            (1.0, 1.0, 0.0, 0.0),
            (1.5, 1.0, (1.5 / 1.0) - 1, 0.5),
            (3.0, 1.5, 1.0, 1.5),
            (2.0, 3.0, (2.0 / 3.0) - 1, -1.0),
        ]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)

        actual_df = add_inflation_columns(test_df)
        self.assert_spark_dataframes_equal(expected_df, actual_df)
