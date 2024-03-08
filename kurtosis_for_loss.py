import numpy as np
from scipy.stats import kurtosis


# Generate some random data
np.random.seed()
data = np.random.normal(loc=5, scale=2, size=1000)  # Normally distributed data

# Calculate the mean and standard deviation of the entire distribution
mean = np.mean(data)
std_dev = np.std(data)

# Calculate the kurtosis of the entire distribution
full_distribution_kurtosis = kurtosis(data)

# Identify the subset of data points that fall on the right side of the mean
right_tail_data = data[data > mean]

# Calculate the kurtosis of the right tail subset
right_tail_kurtosis = kurtosis(right_tail_data)

# Identify the subset of data points that fall on the left side of the mean
left_tail_data = data[data < mean]

# Calculate the kurtosis of the left tail subset
left_tail_kurtosis = kurtosis(left_tail_data)

print("Kurtosis of the entire distribution:", full_distribution_kurtosis)
print("Kurtosis of the right tail:", right_tail_kurtosis)
print("Kurtosis of the left tail:", left_tail_kurtosis)
