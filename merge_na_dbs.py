import argparse
import pandas as pd

NA_15_FILE="na_15_small.csv"
NA_18_FILE="na_18.csv"
NA_25_FILE="na_25.csv"
NA_30_FILE="na_30_NY_short.csv"

OUTPUT_FILE="na_30_ny_merged.csv"

FIELD_IP_START = "ip_start"
FIELD_IP_START2 = "ip_start2"
FIELD_IP_START3 = "ip_start3"
FIELD_IP_START4 = "ip_start4"

class MergeHandler():
    def __init__(self):
        self.stuff = None

    def parse_na_30_ip_ranges(self):
        print(f"parse_na_30_ip_ranges, file: {NA_30_FILE}")
# ip_start,ip_end,pp_country,pp_region,pp_city,pp_conn_speed,pp_conn_type,pp_metro_code,pp_latitude,pp_longitude,pp_postal_code,pp_postal_ext,pp_country_code,pp_region_code,pp_city_code,pp_continent_code,pp_two_letter_country,pp_internal_code,pp_area_codes,pp_country_conf,pp_region_conf,pp_city_conf,pp_postal_conf,pp_gmt_offset,pp_in_dst,pp_timezone_name,Unused-1
        self.df_ip_ranges = pd.read_csv(NA_30_FILE, sep=',')
        self.df_ip_ranges.set_index(FIELD_IP_START, inplace=True)

    def parse_na_15_companies(self):
        print(f"parse_na_15_companies, file: {NA_15_FILE}")
# ip_start2;ip_end;company_name;Unused-2
        self.df_company = pd.read_csv(NA_15_FILE, sep=';')
        self.df_company.set_index(FIELD_IP_START, inplace=True)

    def parse_na_18_naics(self):
        print(f"parse_na_18_naics, file: {NA_18_FILE}")
# ip_start3,ip_end,naics_code,Unused-3
        self.df_naics = pd.read_csv(NA_18_FILE, sep=',')
        self.df_naics.set_index(FIELD_IP_START, inplace=True)

    def parse_na_25_orgs(self):
        print(f"parse_na_25_orgs, file: {NA_25_FILE}")
# ip_start4,ip_end,organization_name,Unused-4
        self.df_org = pd.read_csv(NA_25_FILE, sep=',')
        self.df_org.set_index(FIELD_IP_START, inplace=True)

    def merge_all(self):
        print(f"merge_all, merging {NA_30_FILE} and: {NA_15_FILE}")
        df_temp1 = self.df_ip_ranges.copy()
        # df_temp1[self.df_company.columns] = self.df_company
        # merge, inner.  No good.  No rows
        # merge, left.  We swallow our ip_start field
        df_temp1 = pd.merge(self.df_ip_ranges, self.df_company,
            on=FIELD_IP_START, how="left")
        df_temp2 = df_temp1.drop(["pp_country", "pp_region", "pp_city", "pp_metro_code", "pp_postal_code", "pp_postal_ext", 
            "pp_country_code", "pp_region_code", "pp_city_code", "pp_continent_code", "pp_two_letter_country", 
            "pp_internal_code", "pp_area_codes", "pp_country_conf", "pp_region_conf", "pp_city_conf", "pp_postal_conf",
            "pp_gmt_offset", "pp_in_dst", "pp_timezone_name", "Unused-1", "ip_end_y", "Unused-2"], axis=1)

        print(f"merge_all, merging {NA_30_FILE}/{NA_15_FILE} with: {NA_18_FILE}")
        df_temp1 = self.df_ip_ranges.copy()
        # axis = 1 means drop the column, not the row
        df_temp3 = pd.merge(df_temp2, self.df_naics, on=FIELD_IP_START, how="left")
        df_temp4 = df_temp3.drop(["ip_end", "Unused-3"], axis=1)

        df_temp4.to_csv(OUTPUT_FILE)
        print(f"Wrote file: {OUTPUT_FILE}")

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
    try:
        mh = MergeHandler()
        mh.parse_na_30_ip_ranges()
        mh.parse_na_15_companies()
        mh.parse_na_18_naics()
        mh.parse_na_25_orgs()
        mh.merge_all()
    except Exception as Argument:
        print(f"Exception: {Argument} occurred")

if __name__ == '__main__':
    main()