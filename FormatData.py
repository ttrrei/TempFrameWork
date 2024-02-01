
class FormatData:
    def __init__(self):
        pass


    def normalize_ez (self, array):
        data_float = [float(x) for x in array]

        data_avg = sum(data_float) / len(data_float)

        im_data = [(x - data_float[-1]) / data_avg for x in data_float]
        ds_data = [abs(x) for x in im_data]

        data_dis = max(ds_data)

        return [x / data_dis * len(data_float) for x in im_data]


    def normalize_sz (self, array):
        data_float = [float(x) for x in array]

        data_avg = sum(data_float) / len(data_float)

        im_data = [(x - data_float[0]) / data_avg for x in data_float]
        ds_data = [abs(x) for x in im_data]

        data_dis = max(ds_data)

        return [x / data_dis  for x in im_data]