import datetime
import warnings
import time
import os

from src.tools.manager import Manager
warnings.filterwarnings("ignore")

# Set environment variables
os.environ['NAMESPACE'] = 'AFDC_API'
os.environ['API_NAME'] = 'nrel_api_key'

# Start timing the script
start_time = time.time()

# MAIN SCRIPT
if __name__ == "__main__":
    manager = Manager()


# Stop timing the script
end_time = time.time()
total_time = end_time - start_time
print(f'The Script took {datetime.timedelta(seconds=total_time)} to finish.')
