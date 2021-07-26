import os
import io
import csv
import pathlib


class Output(object):
    def save_csv(self, data: list, path: str):
        """
        Saves data on a file with the csv format.
        :data: List representing data to save.
        :path: path where to save the data.
        """
        pathlib.Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True) 
        with io.open(path, 'w') as csvfile:
            spamwriter = csv.writer(csvfile,  delimiter=';')
            for row in data:
                spamwriter.writerow(row)
