from typing import List

import pandas as pd
import numpy as np

# from pathfiles import path

pd.set_option("display.max_rows", None, "display.max_columns", None)


class Dataframe:

    def __init__(self, path: str):
        """object
        intilized the path value into self object so
        :rtype: none
        """
        self.path = path

        ':arg:files path '
        ':constructor:'

    def createDataframe(self):
        """
        this function create new data frame into the pandas
        :self -as object of dataframe ;
        :read_csv function read csv files
        :pd.dataframe - make data frame
        :data print the dataframe
        """
        df = pd.read_csv(self.path)
        data = pd.DataFrame(df)
        return data

    def print_All_Columns(self, data):
        column = data.columns
        print(column)
        """
       this function print all columns nanme
        :rtype: object
        """

    def renameColumn(self, data, clo_Name: str, exit_Col: str):
        """[summary]

        Args:
            data ([DataFrame]): [this is the datasets where we want to manipulates]
            clo_Name (str): [new column name where we want name ]
            exit_Col (str): [exts columns name where we rename the column]

        Returns:
            [DataFrame]: [After rename we retunrn the dataframe]
        """
        return data.rename(columns={exit_Col: clo_Name}, inplace=True)

    # df.insert(col_index_position, “Col_Name”, Col_Values_As_List, True) statement.
    def add_new_column(self, data, col_index_position, col_Name, col_values_As_List):
        """[this function add new column in our datasets]

        Args:
            data ([DataFrame]): [input data]
            col_index_position ([int]): [which position we want put the column]
            col_Name ([str]): [name of the column u want to put ]
            col_values_As_List ([list]): [efult value u want put in the datasets]

        Returns:
            [DataFrame]: [returning dataframe after new column]
        """
        return data.insert(col_index_position, col_Name, col_values_As_List, True)

    # DataFrame.drop(labels=None, axis=0, index=None, columns=None, level=None, inplace=False, errors='raise')[source]¶
    def dropColumn(self, data, labels=None, axis=0, index=None, columns=None, level=None, inplace=False,
                   errors='raise'):
        """[remove the column from our datasets ]

        Args:
            data ([DataFrame]): [input data]
            labels ([str], optional): [description]. Defaults to None.
            axis (int, optional): [if want insert value row or column row =0 or column=1 ]. Defaults to 0.
            index ([int], optional): [which index are u put the data]. Defaults to None.
            columns ([str], optional): [name of the column ]. Defaults to None.
            level ([type], optional): [description]. Defaults to None.
            inplace (bool, optional): [there are two options in this true means inplace or False means not inplace]. Defaults to False.
            errors (str, optional): [description]. Defaults to 'raise'.

        Returns:
            [DataFrame]: [returning the dataFrame]
        """
        return data.drop(labels, axis, index, columns, level, inplace, errors)

    # DataFrame.update(other, join='left', overwrite=True, filter_func=None, errors='ignore')
    def Updatecolumn(self, data, join='left', overwrite=True, filter_func=None, errors='ignore'):
        """[summary]

        Args:
            data ([type]): [description]
            join (str, optional): [description]. Defaults to 'left'.
            overwrite (bool, optional): [description]. Defaults to True.
            filter_func ([type], optional): [description]. Defaults to None.
            errors (str, optional): [description]. Defaults to 'ignore'.

        Returns:
            [type]: [description]
        """
        return data.update(join, overwrite, filter_func, errors)

    def concatnatingDataSet(self, data, other_Data):
        """[summary]

        Args:
            data ([type]): [description]
            other_Data ([type]): [description]

        Returns:
            [type]: [description]
        """
        lst = [data, other_Data]
        new_data = pd.concat(lst)
        return new_data

    # DataFrame.astype(dtype, copy=True, errors='raise')
    def changeDtypeOFColumn(self, data, dtype: dict, copy=True, errors='raise'):
        """[summary]

        Args:
            data ([type]): [description]
            dtype (dict): [description]
            copy (bool, optional): [description]. Defaults to True.
            errors (str, optional): [description]. Defaults to 'raise'.

        Returns:
            [type]: [description]
        """
        return data.astype(dtype, copy, errors)


# data = {'Name': ['Jai', 'Princi', 'Gaurav', 'Anuj'],
#  'Age': [27, 24, 22, 32],
#  'Address': ['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
#   'Qualification': ['Msc', 'MA', 'MCA', 'Phd']}
pathfiles = "/home/impressico/Desktop/Python/pysparT/Datasets/empdata.csv"

dataframe = Dataframe(pathfiles)
data = dataframe.createDataframe()
dataframe.print_All_Columns(data)
print(dataframe.renameColumn(data, "DateOfBirth", "DOB"))
print(data.columns)
dataframe.add_new_column(data, 10, "Country", ["US"] * 311)
print(data.columns)
print(data.head())
dataframe.changeDtypeOFColumn(data, {"EmpID": "int"})
dta=data["EmpID"]
print("_________________________________________",dta)
type(data)
print((data.columns))
