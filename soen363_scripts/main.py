import re
from pathlib import Path
import pandas as pd
import glob
import csv


def reformat_dat_files(folder_name):
    pathlist = Path(folder_name).glob('*.dat')
    for path in pathlist:
        csv_filename = str(path).replace(".dat", ".csv")
        # NEEDS WORK, THIS GETS RID OF ALL SPACES
        dat_content = [line.strip().split() for line in open(path, encoding="utf8").readlines()]

        # write it as a new CSV file
        with open(csv_filename, "w") as f:
            writer = csv.writer(f)
            writer.writerows(dat_content)


def create_filtered_csv(input_file, output_file, indices):
    """
    :param input_file: path of file that will be partially copied
    :param output_file: path of output file
    :param indices: indices of each row we want to copy in the new file
    :return: null
    """
    counter = 0
    with open(input_file,'r') as input_csv, open(output_file,'w', newline='') as output_csv:
        reader = csv.reader(input_csv)
        writer = csv.writer(output_csv)
        for row in reader:
            out_row = []
            for index in indices:
                out_row.append(row[index])
            writer.writerow(out_row)
            # counter += 1
            # if counter >51:
            #     break


def merge_csv(folder_path):
    # todo find a better way to merge files, pandas turns ints into nums like 1.0
    filenames = [i for i in glob.glob(folder_path + '/*source.csv')]
    # combine all files in the list
    csvs = [pd.read_csv(f, error_bad_lines=False, warn_bad_lines=True) for f in filenames]

    combined_csv = pd.concat(csvs)
    # export to csv
    combined_csv.to_csv(folder_path + "/combined_csv.csv", index=False)


def verify_primary_key(filename, interval):
    counter = 0
    max = 0
    with open(filename,'r') as input_csv:
        reader = csv.reader(input_csv)
        for row in reader:
            counter += 1
            if counter % interval == 0:
                print(row)
                # print(row[0])
                # if int(row[0]) < max:
                #     # the id looped! probably an overflow!
                #     print(row[0])
                #     raise Exception()
                # max = int(row[0])

def override_primary_key(filename):
    out = filename.replace(".csv", "_overridden.csv")
    counter = 0
    with open(filename, 'r') as input_csv, open(out, 'w', newline='') as output_csv:
        reader = csv.reader(input_csv)
        writer = csv.writer(output_csv)
        for row in reader:
            row[0] = counter
            counter += 1
            writer.writerow(row)


def correct_csv(filename, problematic_indices=None):
    """
    Casts floats to ints at the problematic indices.
    :param problematic_indices:
    :param filename:
    :return: void, but creates a new file in same directory
    """
    if problematic_indices is None:
        problematic_indices = [12, 13, 14, 15, 17, 18, 19]
    out = filename.replace(".csv", "_corrected.csv")
    first_line = True
    with open(filename, 'r') as input_csv, open(out, 'w', newline='') as output_csv:
        # cast some columns (12 - 15, 17-19) to ints
        reader = csv.reader(input_csv)
        writer = csv.writer(output_csv)
        for row in reader:
            if first_line:
                first_line = False
                continue
            for index in problematic_indices:
                # a = row[index]
                try:
                    row[index] = int(float(row[index]))
                except:
                    pass
            writer.writerow(row)


def correct_date(filename, indices=[]):
    out = filename.replace(".csv", "_date_fixed.csv")
    with open(filename, 'r') as input_csv, open(out, 'w', newline='') as output_csv:
        reader = csv.reader(input_csv)
        writer = csv.writer(output_csv)
        for row in reader:
            for index in indices:

                if ':' not in row[index] or '/' not in row[index]:
                    row[index] = ''
                # formatting the date section
                try:
                    date = row[index][:10]
                    date_elements = date.split('/')
                    new_date = date_elements[2]+'-'+date_elements[0]+'-'+date_elements[1]
                    row[index] = row[index].replace(date, new_date)
                except IndexError:
                    pass
                # replacing AM/PM  (formatting the time section)
                if " AM" in row[index]:
                    row[index] = row[index].replace(" AM", "")
                else:
                    # (\d\d) denotes group 1, which is returned, it's the hours
                    try:
                        wrong_hours = re.search("\s(\d\d):", row[index]).group(1)
                        right_hours = int(wrong_hours) + 12
                        if right_hours == 24:
                            right_hours = 0
                        row[index] = row[index].replace(" PM", "").replace(" "+wrong_hours+":", " "+str(right_hours)+":")
                    except AttributeError:
                        # skip if the row is missing a date
                        pass
            # print(row)
            writer.writerow(row)

def remove_bad_datatype(filename, indices, datatype=int):
    out = filename.replace(".csv", "_filtered.csv")
    with open(filename, 'r') as input_csv, open(out, 'w', newline='') as output_csv:
        reader = csv.reader(input_csv)
        writer = csv.writer(output_csv)
        for row in reader:
            for index in indices:
                try:
                    if type(datatype(row[index])) is not datatype:
                        row[index] = None
                except ValueError:
                    row[index] = None
            writer.writerow(row)


if __name__ == '__main__':

    dataset_folder_name = 'X:/363_datasets/ChicagoCrime'
    # merge_csv(dataset_folder_name)
    # override_primary_key('X:/363_datasets/ChicagoCrime/combined_csv.csv')
    source_file = 'X:/363_datasets/ChicagoCrime/combined_csv_overridden_corrected_date_fixed_filtered_filtered.csv'
    # correct_csv(source_file)
    # correct_date(source_file, indices=[4, 20])

    # add +1 to the index from the csv file as pandas appends an index at the beginning
    # structure of source file
    # index,Unnamed: 0,ID,Case Number,Date(4),Block,IUCR,Primary Type,Description,Location Description(9),Arrest,
    # Domestic,Beat(12),District,Ward,Community Area,FBI Code(16),X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location(23)

    # create_filtered_csv(
    #     source_file,
    #     dataset_folder_name + '/Chicago_Crimes_Crimes.csv',
    #     [0, 3, 6, 7, 8, 10, 11, 16]
    # )
    # create_filtered_csv(
    #     source_file,
    #    dataset_folder_name + '/Chicago_Crimes_Date.csv',
    #     [0, 4, 19, 20]
    # )
    # create_filtered_csv(
    #     source_file,
    #     dataset_folder_name + '/Chicago_Crimes_Location.csv',
    #     [0, 9, 17, 18, 21, 22]
    # )
    # create_filtered_csv(
    #     source_file,
    #     dataset_folder_name + '/Chicago_Crimes_Police_Area.csv',
    #     [0, 5, 12, 13, 14, 15]
    # )
    # remove_bad_datatype(source_file, [17, 18], datatype=int)
    create_filtered_csv(
        source_file,
        dataset_folder_name + '/phase2.csv',
        list(range(3, 23))
    )

    # verify_primary_key(dataset_folder_name + '/Chicago_Crimes_Date.csv',1602848)



