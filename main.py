import datetime
import warnings
import time

warnings.filterwarnings("ignore")
start_time = time.time()

if __name__ == "__main__":
    pass

end_time = time.time()
total_time = end_time - start_time
print(f'The Script took {datetime.timedelta(seconds=total_time)} to finish.')
