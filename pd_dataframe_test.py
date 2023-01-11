import numpy as np
import pandas as pd

np.random.seed(1618033)

# Set 3 axis labels/dims
years = np.arange(2000, 2010)  # Years
samples = np.arange(0, 20)  # Samples
patients = np.array(["patient_%d" % i for i in range(0, 3)])  # Patients

# Create random 3D array to simulate data from dims above
A_3D = np.random.random((years.size, samples.size, len(patients)))  # (10, 20, 3)

# Create the MultiIndex from years, samples and patients.
midx = pd.MultiIndex.from_product([years, samples, patients])

# Create sample data for each patient, and add the MultiIndex.
patient_data = pd.DataFrame(np.random.randn(len(midx), 3), index=midx)

patient_data.head()


midx = pd.MultiIndex.from_product([years, samples])

# Create sample data for each patient, and add the MultiIndex.
patient_data = pd.DataFrame(np.random.randn(len(midx), 3), index=midx, columns=patients)

patient_data.head()

fourth_level = np.array(["level_member_%d" % i for i in range(0, 5)])

midx = pd.MultiIndex.from_product([years, samples])
mcol = pd.MultiIndex.from_product([patients, fourth_level])

patient_data = pd.DataFrame(np.random.randn(len(midx), len(mcol)), index=midx, columns=mcol)

patient_data.head()

midx = pd.MultiIndex.from_product([years])
mcol = pd.MultiIndex.from_product([patients])

patient_data = pd.DataFrame(np.random.randn(len(midx), len(mcol)), index=midx, columns=mcol)

print(patient_data[:20])
