from enum import Enum
import os
import string
import argparse
import pandas as pd

DATA_DIR = "./Data"

MERGED_FILE="na_30_ny_merged.csv"
SRS_FILE="NY_Corporate.csv"

SRS_COMPANY_NAME = "srs_company_name"
SRS_ISSUER_ID = "srs_issuer_id"
SRS_LATITUDE = "srs_latitude"
SRS_LONGITUDE = "srs_longitude"
SRS_STRENGTH_MATCH = "srs_strength"

class MatchStrength(Enum):
    LOCATION = 1,
    ORG_NAME = 2,
    COMPANY_NAME = 3;

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

    def match_name(self, issuer_first_word, merged_name):
        if type(merged_name) == "string":
            no_punctuation = merged_name.translate(string.punctuation)
            first_word = no_punctuation.split()[0]
            return issuer_first_word == first_word
        return False

    def process_found(self, match_strength, index_merged, srs_row):
        self.df_merged.at[index_merged, SRS_COMPANY_NAME] = srs_row.IssuerName
        self.df_merged.at[index_merged, SRS_ISSUER_ID] = srs_row.IssuerID
        self.df_merged.at[index_merged, SRS_LATITUDE] = srs_row.latitude
        self.df_merged.at[index_merged, SRS_LONGITUDE] = srs_row.longitude
        self.df_merged.at[index_merged, SRS_STRENGTH_MATCH] = match_strength.value

    def matching_location(self, srs_row, rounded_latitude, rounded_longitude, indices_this_company):
        found = False
        issuer_name = srs_row.IssuerName.lower()
        # asset_name = srs_row.AssetName.lower()
        # print(f"resolve_company(), SRS name: ({{rounded_longitude}, {rounded_latitude}): {issuer_name}, {asset_name}")
        subset_df = self.df_merged.iloc[indices_this_company]
        if subset_df.shape[0] == 1:
            # print(f"m_l().1, srs_row = {issuer_name }")
            # Only matching row, call it a match
            self.process_found(MatchStrength.LOCATION, indices_this_company[0], srs_row)
            found = True
        else:
            issuer_first_word = issuer_name.lower().split()[0]
            for index, merged_row in self.df_merged.iterrows():
                if self.match_name(issuer_first_word, merged_row.company_name):
                    print(f"m_l().2, srs_row = {issuer_name}, company_name = {merged_row.company_name}")
                    process_found(MatchStrength.COMPANY_NAME, index, srs_row)
                    found = True
                    break
                elif self.match_name(issuer_first_word, merged_row.organization_name):
                    print(f"m_l().3, srs_row = {issuer_name}, org_name = {merged_row.organization_name}")
                    process_found(MatchStrength.ORG_NAME, index, srs_row)
                    found = True
                    break
        return found
        

    def resolve_companies(self):
        num_rows = self.df_merged.shape[0]
        print(f"resolve_companies(), num_rows = {num_rows}")
        null_list = [None for _ in range(num_rows)]
        self.df_merged[SRS_COMPANY_NAME] = null_list
        self.df_merged[SRS_ISSUER_ID] = null_list
        self.df_merged[SRS_LATITUDE] = null_list
        self.df_merged[SRS_LONGITUDE] = null_list
        self.df_merged[SRS_STRENGTH_MATCH] = null_list
        #print(f"resolve_companies(), columns:")
        #print(self.df_merged.columns)

        companies_found = 0
        # for row in self.df_srs.itertuples():
        for index, srs_row in self.df_srs.iterrows():
            # str_latitude = row.latitude
            rounded_latitude = round(float(srs_row.latitude), 2)
            # str_longitude = row.longitude
            rounded_longitude = round(float(srs_row.longitude), 2)
            indices_this_company = []
            for index2, merged_row in self.df_merged.iterrows():
                digel_latitude = float(merged_row.pp_latitude)
                digel_longitude = float(merged_row.pp_longitude)
                if (rounded_latitude == digel_latitude and 
                    rounded_longitude == digel_longitude):
                    #print(f"resolve_companies(), FOUND LAT, row[{index2}] = (lat, long) {digel_latitude}, {digel_longitude}")
                    #indices_companies_found.append(index2)
                    indices_this_company.append(index2)
            if len(indices_this_company) > 0:
                print(f"calling r_c_n(), indices = {indices_this_company}")
                if self.matching_location(srs_row, rounded_latitude, rounded_longitude, indices_this_company):
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
            # str2_lat = str(float_latitude)  
            # str2_long = str(float_longitude)  
            # This should work... inner = self.df_merged["pp_latitude"] == str2_lat
            #if index <= 15:
            #    print(f"resolve_companies(), row[{index}] = (lat, ...) {str2_lat}")
            #    # Should be a dataframe of (rows,...) where the type is a boolean
            #    inner = self.df_merged["pp_latitude"] == str2_lat
            #    print(f"    inner.shape = {inner.shape},{inner.value_counts()}")
#            filtered_df1 = self.df_merged[self.df_merged["pp_latitude"] == str2_lat]
#            if filtered_df1.shape[0] > 0:
#                print(f"resolve_companies(), FOUND LAT, row[{index}] = (lat, long) {str2_lat}, {str2_long}")
#                print(f"resolve_companies(), filtered.shape: {filtered_df1.shape}")
#                companies_found = companies_found + 1
#        for index, row in self.df_merged.iterrows():
#            value = row.pp_latitude
#            print(f"resolve_companies(), merged_row[{index}] = (lat, ...) {value}")
#            if (index >= 20):
#                break
