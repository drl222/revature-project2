# Instructions:
- Create a Spark Application that process Covid data.
Your project  should involve some analysis of covid data(Every concept of spark from rdd, dataframes, sql, dataset and optimization methods should be included, persistence also).  The final expected output is different trends that you have observed as part of data collectivley and how can WHO make use of these trends to make some useful decisions.
- Lets the P2 Demo, have presentation with screen shots and practical demo for at least one of your trends.

# Data we have:
- covid_19_data.csv (still worldwide)
	- SNo: ID number?
	- ObservationDate
	- Province/State
	- Country/Region
	- Last Update
	- Confirmed
	- Deaths
	- Recovered
- time_series_covid_19_confirmed_US.csv, time_series_covid_19_deaths_US.csv
	- UID: ID number?
	- iso2: country code 2 letter
	- iso3: country code 3 letter
	- code3: ??? (unknown integer)
	- FIPS: number that identifies a region in the US
	- Admin2: City name
	- Province_State: state name
	- Country_Region: country again
	- Lat: latitude
	- Long_: longitude
	- Combined_Key: "city, state, country" in one string
	- 1/22/20 (...) 5/2/21: total count of deaths up to this date
- time_series_covid_19_confirmed.csv, time_series_covid_19_deaths.csv, time_series_covid_19_recovered.csv
	- Province/State
	- Country/Region
	- Lat
	- Long
	- (dates)

# Trends we want to discover
- Show the data again, but pretty this time
- Compare states: COVID cases vs. deaths
- Likewise, compare countries
	- geographical data: islands, etc.
		- latitude (as a proxy for temperature)
		- seasons: northern vs. southern hemispheres
		- continents
	- ratio confirmed/deaths when comparing when the first confirmed date is later vs earlier
- Likewise, cities
- Likewise, compare by month
- Look at peaks of cases vs. deaths/recovered within the same area (see how long it takes b/t positive case and death/recovery)
	- if we want to be fancy, we can do correlations of the full time series rather than just peaks

# What we know how to do
- sum, average, difference, ratio (division)
- we have both time and space information

- What we want to find out: "X factor influence Y result"
- possible X factors
	- > Tanka: latitude/northern vs. southern hemisphere/seasons
	- > Dylan: initial appearance date of first confirmed case
	- > Derrick: height/date of first peak
	- Tanka: comparing US vs. world-except-US average
	- Dylan: by continent
	- Derrick: whether or not there's more than one peak
	
	- population (external data!)
	- mask mandates (external data!)
- how to measure influence
	- throw X, Y on a graph and draw a trendline
	- cross-correlation of graphs of some kind?
- Y result
	- total confirmations/deaths/recoveries
		- confirmations/deaths/recoveries per day
	- > death:recovered or death:confirmed ratios

Spark technologies
- RDD: Derrick
- Dataframe: Dylan
- Dataset: Derrick
- SQL: Tanka
We'll throw in optimization and persistence somewhere