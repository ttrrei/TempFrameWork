import sys
from DataOperation import DataOperation
from FormatData import FormatData
from CurvFit import CurvFit


def main(param1, param2):
    do = DataOperation("localhost", "equity", "postgres", "nemo123")
    fd = FormatData()
    cf = CurvFit()

    dataset = do.fetch_data_from_db("select entity, idx, attribute, len as leng, evaluation from "
                                    "tier2.eavt_source where attribute = '"+ param1+"' and len = "+ param2+" limit 1")

    for index, row in dataset.iterrows():
        array = dataset['evaluation'].iloc[index]
        entity = dataset['entity'].iloc[index]
        idx = dataset['idx'].iloc[index]
        attribute = dataset['attribute'].iloc[index]
        leng = dataset['leng'].iloc[index]

        sz = fd.normalize_sz(array)
        ez = fd.normalize_ez(array)

        x1 = list(range(-len(array) + 1, 1))
        x2 = list(range(len(array)))

        do.write_data_to_db(entity, str(idx), attribute, str(leng), (cf.linear_fit(x2, sz)),
                            (cf.quadratic_fit(x2, sz)), (cf.sin_fit(x2, sz)))

    do.close_db_connection()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python main.py <param1> <param2>")
    else:
        param1 = sys.argv[1]
        param2 = sys.argv[2]
        main(param1, param2)
