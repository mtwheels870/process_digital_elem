import os
import argparse
import pandas as pd

DATA_DIR = "./Data"

MERGED_FILE="na_30_ny_merged.csv"
SRS_FILE="NY_Corporate.csv"

SRS_COMPANY_NAME = "srs_company_name"
SRS_ISSUER_ID = "srs_issuer_id"
SRS_LATITUDE = "srs_latitude"
SRS_LONGITUDE = "srs_longitude"

FIELD_IP_START = "ip_start"

PLUS_SRS_FILE="na_30_ny_srs.csv"

class DigitalElementLoader():
    def __init__(self):
        self.stuff = None

    def load_merged(self):
        full_path = os.path.join(DATA_DIR, MERGED_FILE)
        self.df_merged = pd.read_csv(full_path, sep=',', na_values="None", dtype={"latitude": float, "longitude": float})
        shape = self.df_merged.shape
        print(f"load_merged(), file: {MERGED_FILE}, shape = {shape}")


    def load_SRS(self):
        full_path = os.path.join(DATA_DIR, SRS_FILE)
        self.df_srs = pd.read_csv(full_path, sep=',', dtype='str', na_values="None")
        shape = self.df_srs.shape
        print(f"load_SRS(), file: {SRS_FILE}, shape = {shape}")

    def resolve_companies(self):
        num_rows = self.df_merged.shape[0]
        print(f"resolve_companies(), num_rows = {num_rows}")
        null_list = [None for _ in range(num_rows)]
        self.df_merged[SRS_COMPANY_NAME] = null_list
        self.df_merged[SRS_ISSUER_ID] = null_list
        self.df_merged[SRS_LATITUDE] = null_list
        self.df_merged[SRS_LONGITUDE] = null_list
        print(f"resolve_companies(), columns:")
        print(self.df_merged.columns)

        companies_found = 0
        # for row in self.df_srs.itertuples():
        for index, row in self.df_srs.iterrows():
            str_latitude = row.latitude
            float_latitude = round(float(row.latitude), 2)
            str_longitude = row.longitude
            float_longitude = round(float(row.longitude), 2)
            str2_lat = str(float_latitude)  
            str2_long = str(float_longitude)  
            filtered_df1 = self.df_merged[self.df_merged["pp_latitude"] == str2_lat]
            if filtered_df1.shape[0] > 0:
                print(f"resolve_companies(), FOUND LAT, row[{index}] = (lat, long) {str2_lat}, {str2_long}")
                print(f"resolve_companies(), filtered.shape: {filtered_df1.shape}")
            # filtered_df2 = filtered_df2["pp_longitude" == str2_long]
#            index_result = 0
#            for row in filtered_df1.itertuples:
#                company_name = filtered_df1["company_name"]
#                naics_code = filtered_df1["naics_code"]
#                organization_name = filtered_df1["organization_name"]
#                print(f"resolve_companies(), [{index_result}]: {company_name}, {naics_code}, {organization_name}")
#                index_result = index_result + 1
            self.df_merged.at[index, SRS_COMPANY_NAME] = row.IssuerName
            self.df_merged.at[index, SRS_ISSUER_ID] = row.IssuerID
            self.df_merged.at[index, SRS_LATITUDE] = row.latitude
            self.df_merged.at[index, SRS_LONGITUDE] = row.longitude
            if index >= 10:
                break
            companies_found = companies_found + 1
        full_path = os.path.join(DATA_DIR, PLUS_SRS_FILE)
        self.df_merged.to_csv(full_path)
        print(f"Wrote file: {full_path}, {companies_found} companies found")

def main():
    parser = argparse.ArgumentParser(description="calculate X to the power of Y")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true")
    group.add_argument("-q", "--quiet", action="store_true")
    # parser.add_argument("x", type=int, help="the base")
    # parser.add_argument("y", type=int, help="the exponent")
    args = parser.parse_args()
#    answer = args.x**args.y

#    if args.quiet:
#        print(answer)
#    elif args.verbose:
#        print(f"{args.x} to the power {args.y} equals {answer}")
#    else:
#        print(f"{args.x}^{args.y} == {answer}")
    #try:
    mh = DigitalElementLoader()
    mh.load_merged()
    mh.load_SRS()
    mh.resolve_companies()
#    except Exception as Argument:
#        print(f"Exception: {Argument} occurred")

if __name__ == '__main__':
    main()
