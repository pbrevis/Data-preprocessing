# Loading libraries
library(dplyr)
library(stringr)

rm(list = ls()) # Cleaning my global environment


################################################################################
## STEP 1: READING SOURCE DATA
################################################################################

# Reading FAOSTAT data file
fao_df <- read.csv("FAOSTAT_data_en_3-23-2024.csv")
glimpse(fao_df)

# Making list of unique Elements and Items in 'fao_df'
print(unique(fao_df$Element))
print(unique(fao_df$Item))


# Reading "countries_ids.csv" file
countryid_df <- read.csv("countries_ids.csv")
glimpse(countryid_df)


################################################################################
## STEP 2: INSPECTING COUNTRY NAMES IN BOTH DATASETS
################################################################################


# Counting number of unique country names in 'fao_df' column 'Area'
n_distinct(fao_df$Area)

# Making list of unique country names in 'fao_df' (column: Area)
fao_area_names <- unique(fao_df$Area)
length(fao_area_names)
head(fao_area_names)
print(fao_area_names)

# Write list of unique area (country) names into csv
write.csv(fao_area_names, "fao_area_names.csv")



# Counting number of unique country codes in 'countryid_df' column 'cca3'
n_distinct(countryid_df$cca3)

# Counting number of unique entity (country) names in 'countryid_df'
# column 'entity_id'
n_distinct(countryid_df$entity_id)

# Making list of unique entity names in 'countryid_df' (column: entity_id)
entity_names <- unique(countryid_df$entity_id)
length(entity_names)
head(entity_names)
print(entity_names)


################################################################################
## STEP 3: MANIPULATING countryid_df DATASET
################################################################################

# Adding additional column ('entity_id2') where country names (from 'entity_id')
# appear in Title case and without hyphen
countryid_df$entity_id2 <- gsub("-", " ", countryid_df$entity_id)
                                 # replace hyphen for space
countryid_df$entity_id2 <- stringr::str_to_title(countryid_df$entity_id2)
                                 # transform string into Title case

# Quick glimpse at dataframe
glimpse(countryid_df)
print(countryid_df$entity_id2)

################################################################################
## STEP 4: MERGING DATAFRAMES WITH FUNCTION left_join
################################################################################

# Merging both dataframes by left join
merged_df <- left_join(fao_df, countryid_df, by =c('Area'='entity_id2'))

# Quick glimpse at joined dataframe
glimpse(merged_df)


################################################################################
## STEP 5: INSPECTING MERGED DATAFRAME
################################################################################


countries_missing_label <- merged_df[is.na(merged_df$cca3), "Area"]
length(countries_missing_label)

# Counting number of unique country names in 'countries_missing_label'
n_distinct(countries_missing_label)

# Making list of unique country names in 'countries_missing_label'
countries_missing_label <- unique(countries_missing_label)
length(countries_missing_label)
head(countries_missing_label)
print(countries_missing_label)

write.csv(countries_missing_label, "output_countries_missing_label.csv")

################################################################################
## STEP 6: CREATING NEW CSV FILE WITH cca3 AND entity_id FOR COUNTRIES MISSING
## LABELS
################################################################################

# Reading 'new_countries_ids.csv' data file
# This file was created manually
# First column with row numbers was dropped
missing_ids_df <- read.csv("new_countries_ids.csv", colClasses = c("NULL", NA, NA, NA))
glimpse(missing_ids_df)
print(missing_ids_df)

# Frequency table of missing values NA
table(is.na(missing_ids_df$cca3))


################################################################################
## STEP 7: MERGING NEW DATAFRAME BY LEFT OUTER JOIN
################################################################################

# Left outer join
merged_df2 <- merge(x = merged_df, y = missing_ids_df, by = "Area", all.x = TRUE)

glimpse(merged_df2)

merged_df2$cca3 <- ifelse(is.na(merged_df2$cca3.x), merged_df2$cca3.y,
                          merged_df2$cca3.x)
merged_df2$entity_id <- ifelse(is.na(merged_df2$entity_id.x), merged_df2$entity_id.y,
                          merged_df2$entity_id.x)
# Merging columns...
glimpse(merged_df2)

# then removing redundant columns
merged_df3 <- subset(merged_df2, select = -c(cca3.x, entity_id.x, cca3.y, entity_id.y))
glimpse(merged_df3)

n_distinct(merged_df3$Area)

# Frequency table of missing values NA
table(is.na(merged_df3$Area))

n_distinct(merged_df3$cca3)

# Frequency table of missing values NA
table(is.na(merged_df3$cca3))

n_distinct(merged_df3$entity_id)

# Frequency table of missing values NA
table(is.na(merged_df3$entity_id))



################################################################################
## STEP 8: INSPECTING NEW MERGED DATAFRAME
################################################################################


countries_missing_label2 <- merged_df3[is.na(merged_df3$cca3), "Area"]
length(countries_missing_label2)

# Counting number of unique country names in 'countries_missing_label2'
n_distinct(countries_missing_label2)

# Making list of unique country names in 'countries_missing_label2'
countries_missing_label2 <- unique(countries_missing_label2)
length(countries_missing_label2)
head(countries_missing_label2)
print(countries_missing_label2)

write.csv(merged_df3, "output_final_dataset.csv")



