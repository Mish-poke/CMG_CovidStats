_version_ = 1.44

#<>

# V1.32
# informationAboutLastVersion = "### NEW in V " + str(_version_) + chr(10) + \
# " 1. add timestamp and the end of the filename to better keep track when the tool was used / data was pulled in case of changes during the day" + chr(10) + \
# " 2. save another copy of raw & prepared datasets in the costa group shared drive WITHOUT name as new MASTER file that is overwritten every time" + chr(10) + \

# V1.33
# informationAboutLastVersion = "### NEW in V " + str(_version_) + chr(10) + \
# " 1. German Districts may show up twice, since we have Stadt and Landkreis with the same name. District BEZ was therefore added" + chr(10) + \

# V1.34
# informationAboutLastVersion = "### NEW in V " + str(_version_) + chr(10) + \
# " 1. add new column in ECDC dataset analysing if countries IR was above or below Italys IR at this day" + chr(10) + \
# "### ### ###"

# V1.35
#informationAboutLastVersion = "### NEW in V " + str(_version_) + chr(10) + \
#" 1. add new column in ECDC dataset analysing if countries IR was above or below Italys IR at this day" + chr(10) + \
#" 2. added Infection Rate Forecast Model" + chr(10) + \
#" 3. added Model to predict crossing of Infection Rate value with IR of Italy" + chr(10) + \
#"### ### ###"
#
# # V1.36
# informationAboutLastVersion = "### NEW in V " + str(_version_) + chr(10) + \
# "when ever ECDC is adding a new country in their list, tool is not crashing anymore." + chr(10) + \
# "the tools shows multiple error messages for the country missing in the Annex20 list." + chr(10) + \
# "for all other countries the tool is running without interruption." + chr(10) + \
# "... last but not least: tool window keeps open at the end." + chr(10) + \
# "### ### ###"

# # V1.42
# informationAboutLastVersion = "### NEW in V " + str(_version_) + chr(10) + \
# "ECDC data is not in use anymore." + chr(10) + \
# "Johns Hopkins Data is main source now." + chr(10) + \
# "Infection Rate is calculated based on country population as per Eurostat database ..." + chr(10) + \
# "... or if not available, as per World-Bank Population data" + chr(10) + \
# "...minor bugfix including the UAE into the JHU IR analytics, even so its as of now an Annex 20 country" + chr(10) + \
# "### ### ###"

# # V1.43
# informationAboutLastVersion = "### NEW in V " + str(_version_) + chr(10) + \
# "Weekly ECDC dataset implemented" + chr(10) + \
# "JHU and ECDC data are now used in parallel" + chr(10) + \
# "### ### ###"

# V1.44
informationAboutLastVersion = "### NEW in V " + str(_version_) + chr(10) + \
"Rework to run outside the CMG network" + chr(10) + \
"### ### ###"


import pandas as pd
# import matplotlib
# import matplotlib.pyplot as plt
import numpy as np
import datetime
import os
import requests
import json
import sys
import math
from datetime import timedelta
import getpass
# from playsound import playsound

yes = {'yes','y', 'ye', ''}
no = {'no','n'}

masterDelimiterFinalFiles = ";"

# ######################################################################################################################
flag_doTheStatsUsing_ECDC = 0
flag_doTheStatsUsing_ECDC_Weekly = 1
flag_doTheStatsUsing_JHU = 1
flag_doTheGermanDistricts_RKI_YESTERDAY = 1
# ######################################################################################################################

dict_possiblePaths = {
	"path_local": 0,
	"path_togetherCMG": 1,
	"path_costaGroupShared": 2,
	"path_TR_HomeLocal": 3
}

dict_masterFileOrSubfileWithTimeStamp = {
	"fileType_master" : 0,
	"fileType_dailyWithTime": 1
}

dict_above_or_belowIR_Italy = {
	"below_IR_Italy": 0,
	"above_IR_Italy": 1,
	"notCalculatedYet": -1
}

dict_ecdc_dataset = {
	"old_daily": 0,
	"new_weekly": 1
}

dict_oneMillionDifferentCountryNames = {
	"Gibraltar": "United Kingdom / Gibraltar",
	"Holy_See": "Holy See",
	"Isle_of_Man": "United Kingdom / Isle of Man",
	"San_Marino": "San Marino",
	"United_Arab_Emirates": "United Arab Emirates",
	"United_Kingdom": "United Kingdom"
}

dict_jhuCountryNamesToBeUpdatedBeforeDataPreparation = {
	"United Kingdom_Gibraltar": "United Kingdom / Gibraltar",
	"United Kingdom_Isle of Man": "United Kingdom / Isle of Man"
}

dict_finalNamesBeforeSavingData = {
	"United Kingdom / Isle of Man": "Isle of Man",
	"United Kingdom / Gibraltar": "Gibraltar"
}

# ######################################################################################################################
#region PBI Folder Path (local vs shared drive) and final file names
dict_flag_pbi_path_finalFolder = {
	0: r"C:\COVID_Reporting\PBI",
	1: r'\\wrfile11\cmg\Together_CMG\Covid19_ECDC_JHU\PBI',
	3: r'E:\001_CMG\010 CMG_Covid\PBI'
}

flag_pbi_name_fileName_ECDC = "COVID_Statistics_ECDC"
flag_pbi_name_fileName_ECDC_Weekly = "COVID_Statistics_ECDC_WEEKLY"
flag_pbi_name_fileName_JHU = "COVID_Statistics_JohnsHopkinsUniversity"
flag_pbi_name_fileName_RKI_District_Yesterday = "COVID_Statistics_RKI_PerDistrict_Yesterday"
flag_pbi_name_fileName_RKI_District_Timeline = "COVID_Statistics_RKI_PerDistrict_Timeline"
#endregion

# ######################################################################################################################
#region ECDC Dataset
flag_ecdc_sourceDataPathFromTheInternet = 'https://opendata.ecdc.europa.eu/covid19/casedistribution/csv'

dict_dataPaths_ECDC = {
	0: r'C:\COVID_Reporting\Data\ECDC',
	1: r'\\wrfile11\cmg\Together_CMG\Covid19_ECDC_JHU\Data\ECDC',
	2: r'\\costafs.costa.it\groupshare\Public\Covid19_ECDC_JHU\Data\ECDC',
	3: r'E:\001_CMG\010 CMG_Covid\002 Daily Data\ECDC'
}

flag_fileName_ecdc = "ECDC"
flag_csvFile_ecdc_delimiter = ","
flag_csvFile_ecdc_decimal = "."

flag_ecdc_date = "dateRep"
flag_ecdc_intDay = "day"
flag_ecdc_intMonth = "month"
flag_ecdc_intYear = "year"
flag_ecdc_newCasesThisDay = "cases"
flag_ecdc_newDeathsThisDay = "deaths"
flag_ecdc_countryLongName = "countriesAndTerritories"
flag_ecdc_countryGeoId = "geoId"
flag_ecdc_countryIsoCode = "countryterritoryCode"
flag_ecdc_officialPopulationDataIn_2019 = "popData2019"
flag_ecdc_continent = "continentExp"
flag_ecdc_cumulative_IR_last_14_days = "Cumulative_number_for_14_days_of_COVID-19_cases_per_100000"
flag_ecdc_cumulative_IR_last_7_days = "ECDC IR 7 days"
flag_ecdc_cumulativeTotalCases = "cumulativeTotalCases"
flag_ecdc_totalNewCases_last_7_days = "new cases last 7 days"
flag_ecdc_totalNewCases_last_14_days = "new cases last 14 days"
flag_ecdc_IR_thisDayInRelationToItalysIR = "IR above(1) or below(0) Italys IR"
flag_ecdc_IR14_deltaPerDay = "IR14 Delta"
flag_ecdc_IR14_avgDeltaLastWeek = "IR14 avg last week"
flag_ecdc_timeLeftBeforeCrossingItaly = "IR14 time before crossing"

flag_calculatedIR7ForECDC = True
#endregion

# ######################################################################################################################
#region WEEKLY ECDC Dataset
flag_ecdcWeekly_sourceDataPathFromTheInternet = 'https://opendata.ecdc.europa.eu/covid19/casedistribution/csv'

dict_dataPaths_ECDC = {
	0: r'C:\COVID_Reporting\Data\ECDC',
	1: r'\\wrfile11\cmg\Together_CMG\Covid19_ECDC_JHU\Data\ECDC',
	2: r'\\costafs.costa.it\groupshare\Public\Covid19_ECDC_JHU\Data\ECDC',
	3: r'E:\001_CMG\010 CMG_Covid\002 Daily Data\ECDC'
}

flag_fileName_ecdcWeekly = "ECDC_WEEKLY"
flag_csvFile_ecdc_delimiter = ","
flag_csvFile_ecdc_decimal = "."

flag_ecdcWeekly_date = "dateRep"
flag_ecdcWeekly_year_week = 'year_week'
flag_ecdcWeekly_cases_weekly = 'cases_weekly'
flag_ecdcWeekly_deaths_weekly = 'deaths_weekly'
flag_ecdcWeekly_countriesAndTerritories = 'countriesAndTerritories'
flag_ecdcWeekly_geoId = 'geoId'
flag_ecdcWeekly_countryterritoryCode = 'countryterritoryCode'
flag_ecdcWeekly_popData2019 = 'popData2019'
flag_ecdcWeekly_continentExp = 'continentExp'
flag_ecdcWeekly_IR14 = 'notification_rate_per_100000_population_14-days'

flag_ecdcWeekly_cumulativeTotalCases = "cumulativeTotalCases"
flag_ecdcWeekly_IR_thisDayInRelationToItalysIR = "IR above(1) or below(0) Italys IR"
#endregion

# ######################################################################################################################
#region JHU Dataset of confirmed cases
flag_jhu_sourceDataPathFromTheInternet = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"

dict_dataPaths_JHU = {
	0: r'C:\COVID_Reporting\Data\JHU',
	1: r'\\wrfile11\cmg\Together_CMG\Covid19_ECDC_JHU\Data\JHU',
	2: r'\\costafs.costa.it\groupshare\Public\Covid19_ECDC_JHU\Data\JHU',
	3: r'E:\001_CMG\010 CMG_Covid\002 Daily Data\JHU'
}

flag_fileName_jhu = "JHU"
flag_csvFile_jhu_delimiter = ","
flag_csvFile_jhu_decimal = "."

flag_jhuSourceFile_Province = 'Province/State'
flag_jhuSourceFile_Country = 'Country/Region'
flag_jhuSourceFile_Lat = 'Lat'
flag_jhuSourceFile_Long = 'Long'

flag_jhu_converted_date = "Date"
flag_jhu_converted_country = "Country"
flag_jhu_converted_totalCasesUntilThisDay = "total cases"
flag_jhu_converted_deltaPreviousDay = "new cases"
flag_jhu_converted_population2019 = "total pop 2019"
flag_jhu_converted_cumulative_IR_7days = "JHU IR 7 days"
flag_jhu_converted_cumulative_IR_14days = "JHU IR 14 days"
flag_jhu_converted_IR7_above_or_belowItaly = "IR7 above(1) or below(0) Italys IR"
flag_jhu_converted_IR14_above_or_belowItaly = "IR14 above(1) or below(0) Italys IR"
flag_jhu_converted_IR7_DeltaPreviousDay = "IR7 Delta"
flag_jhu_converted_IR14_DeltaPreviousDay = "IR14 Delta"
#endregion

# ######################################################################################################################
# region United-Nations Population DB
df_unitedNations = pd.DataFrame()

flag_unPop_LocID = 'LocID'
flag_unPop_Location = 'Location'
flag_unPop_VarID = 'VarID'
flag_unPop_Variant = 'Variant'
flag_unPop_Time = 'Time'
flag_unPop_MidPeriod = 'MidPeriod'
flag_unPop_PopMale = 'PopMale'
flag_unPop_PopFemale = 'PopFemale'
flag_unPop_PopTotal = 'PopTotal'
flag_unPop_PopDensity = 'PopDensity'

dict_flag_unPop_filePath = {
	0: r'C:\COVID_Reporting\Data\Population_Data\United Nations\United_Nations_FinalPopulation_2019.csv',
	1: r'\\wrfile11\cmg\Together_CMG\Covid19_ECDC_JHU\Data\Population_Data\United Nations\United_Nations_FinalPopulation_2019.csv',
	3: r'E:\001_CMG\010 CMG_Covid\002 Daily Data\Population_Data\United Nations\United_Nations_FinalPopulation_2019.csv'
}
# endregion

# ######################################################################################################################
# region Eurostat Population DB

datapath_Eurostat = "https://ec.europa.eu/eurostat/databrowser/view/DEMO_GIND/default/table?lang=en"

df_eurostatPopulation = pd.DataFrame()

flag_eurostat_CountryLabel = 'GEO (Labels)'
flag_eurostat_population = 'population_2020'

dict_flag_eurostat_filePath = {
	0: r'C:\COVID_Reporting\Data\Population_Data\Eurostat\EUROSTAT_2020_preparedData.csv',
	1: r'\\wrfile11\cmg\Together_CMG\Covid19_ECDC_JHU\Data\Population_Data\Eurostat\EUROSTAT_2020_preparedData.csv',
	3: r'E:\001_CMG\010 CMG_Covid\002 Daily Data\Population_Data\Eurostat\EUROSTAT_2020_preparedData.csv'
}
# endregion

# ######################################################################################################################
# region World Bank Population DB

datapath_WorldBank = "https://datatopics.worldbank.org/world-development-indicators/themes/people.html"

df_worldBankPopulation = pd.DataFrame()

flag_WorldBank_CountryName = 'Country Name'
flag_worldBank_CountryCode = 'Country Code'
flag_worldBank_Population_EndOf2019 = "2019"

dict_flag_worldBankPopulation_filePath = {
	0: r'C:\COVID_Reporting\Data\Population_Data\World_Bank\RecentPopulation_World_Bank.csv',
	1: r'\\wrfile11\cmg\Together_CMG\Covid19_ECDC_JHU\Data\Population_Data\World_Bank\RecentPopulation_World_Bank.csv',
	3: r'E:\001_CMG\010 CMG_Covid\002 Daily Data\Population_Data\World_Bank\RecentPopulation_World_Bank.csv',
}
# endregion

# ######################################################################################################################
#region RKI dataset district level YESTERDAY only
flag_fileName_RKI_District_Yesterday = "RKI District Yesterday"
flag_csvFile_rki_yesterday_delimiter = ","
flag_csvFile_rki_yesterday_decimal = "."

dict_dataPaths_RKI_districtYesterday = {
	0: r'C:\COVID_Reporting\Data\RKI_Districts_Yesterday',
	1: r'\\wrfile11\cmg\Together_CMG\Covid19_ECDC_JHU\Data\RKI_Districts_Yesterday',
	2: r'\\costafs.costa.it\groupshare\Public\Covid19_ECDC_JHU\Data\RKI_Districts_Yesterday',
	3: r'E:\001_CMG\010 CMG_Covid\002 Daily Data\RKI_Districts_Yesterday'
}

url_districtsYesterday = \
	"https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_Landkreisdaten/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"
	
flag_rki_districtName = "District Name"
flag_rki_districtStadtLandkreis = "Stadt or Landkreis"
flag_rki_totalCases = "cases"
flag_rki_casesPer100k = "cases per 100k"
flag_rki_cases7Per100k = "IR 7 per 100k"
flag_rki_districtPopulation = "Population"
flag_rki_lastUpdate = "Last Update Date"
flag_rki_agsNumber = "AGS-Number"

dict_featureNames = {
	"rki_districtName": "GEN",
	"rki_districtBEZ": "BEZ",
	"rki_totalCases": "cases",
	"rki_casesPer100k": "cases_per_100k",
	"rki_IR7per100k": "cases7_per_100k",
	"rki_districtPopulation": "EWZ",
	"rki_lastUpdateDate": "last_update",
	"rki_AGS_Number": "AGS"
}
#endregion

# ######################################################################################################################
#region RKI dataset district level timeline
flag_fileName_RKI_District_Timeline = "RKI District Timeline"
flag_csvFile_rki_timeline_delimiter = ","
flag_csvFile_rki_timeline_decimal = "."

dict_dataPaths_RKI_districtTimeline = {
	0: r'C:\COVID_Reporting\Data\RKI_Districts_Timeline',
	1: r'\\wrfile11\cmg\Together_CMG\Covid19_ECDC_JHU\Data\RKI_Districts_Timeline',
	2: r'\\costafs.costa.it\groupshare\Public\Covid19_ECDC_JHU\Data\RKI_Districts_Timeline',
	3: r'E:\001_CMG\010 CMG_Covid\002 Daily Data\RKI_Districts_Timeline'
}

url_districtsTimeline = \
	"https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"
#endregion

# ######################################################################################################################
#region Annex 20 Country List
dict_dataPaths_annex20CountryList = {
	0: r'C:\COVID_Reporting\PBI\ECDC_CountryList_Annex20.xlsx',
	1: r'\\wrfile11\cmg\Together_CMG\Covid19_ECDC_JHU\PBI\ECDC_CountryList_Annex20.xlsx',
	3: r'E:\001_CMG\010 CMG_Covid\PBI\ECDC_CountryList_Annex20.xlsx',
}

flag_annex20_Country = "Country"
flag_annex20_Comment = "Comment"
flag_annex20_Annex20CountryLetter = "Annex 20 Country Letter"
flag_annex20_CountryName_JHU = "Country Name JHU"
flag_annex20_CountryName_Eurostat = "Country Name EUROSTAT"
flag_annex20_CountryName_WorldBank = "Country Name World Bank"

#endregion

# ######################################################################################################################
def func_printThisDataframeHeader(
	thisDF,
	linesToShow,
	prefix = ""
):
	print(chr(10) + "### " + prefix +" DATAFRAME HEADER & first " + str(linesToShow) + " Lines ######################################")
	print(thisDF.head(linesToShow))
	

# ######################################################################################################################
def func_readSourceData_UnitedNations(
	flag_workInTogetherCMG
):
	print(chr(10) + "READ United Nations Population Data @ " + dict_flag_unPop_filePath[flag_workInTogetherCMG])
	
	df_unData = pd.read_csv(
		dict_flag_unPop_filePath[flag_workInTogetherCMG],
		delimiter=";", decimal=",", low_memory=False, error_bad_lines=False
	)
	
	func_printThisDataframeHeader(df_unData, 3)
	
	return df_unData


# ######################################################################################################################
def func_readSourceData_Eurostat(
	flag_workInTogetherCMG
):
	print(chr(10) + "READ Eurostat Population Data @ " + dict_flag_eurostat_filePath[flag_workInTogetherCMG])
	
	df_tempData = pd.read_csv(
		dict_flag_eurostat_filePath[flag_workInTogetherCMG],
		delimiter=";", thousands = ".", low_memory=False, error_bad_lines=False
	)
	
	df_tempData[flag_eurostat_population] = df_tempData[flag_eurostat_population].astype(float)
	
	func_printThisDataframeHeader(df_tempData, 3)
	
	return df_tempData


# ######################################################################################################################
def func_readSourceData_WorldBank(
	flag_workInTogetherCMG
):
	print(chr(10) + "READ World Bank Population Data @ " + dict_flag_worldBankPopulation_filePath[flag_workInTogetherCMG])
	
	df_tempData = pd.read_csv(
		dict_flag_worldBankPopulation_filePath[flag_workInTogetherCMG],
		usecols=[flag_WorldBank_CountryName, flag_worldBank_CountryCode, flag_worldBank_Population_EndOf2019],
		delimiter=",", low_memory=False, error_bad_lines=False
	)
	
	func_printThisDataframeHeader(df_tempData, 3)
	
	return df_tempData

# ######################################################################################################################
def func_readDataFromSourceOrFromHdIfAvailableAlready(
	thisSourceFileFlag,
	amountOfRows = None
):
	print(chr(10) + "READ/UPDATE Source Data for " + thisSourceFileFlag)
	
	df_thisCovidData = pd.DataFrame()
	
	sourceDataPathInTheInternet = func_getSourceDataPath(thisSourceFileFlag)
	
	thisDelimiter, thisDecimal = func_getCSVSeparationStrings(thisSourceFileFlag)
	
	df_thisCovidData = pd.read_csv(
		sourceDataPathInTheInternet,
		delimiter=thisDelimiter, decimal=thisDecimal, nrows=amountOfRows, low_memory=False, error_bad_lines=False
	)
	
	return df_thisCovidData


# ######################################################################################################################
def func_getCSVSeparationStrings(
	thisSourceFileFlag
):
	thisDelimiter = ","
	thisDecimal = "."
	
	if thisSourceFileFlag == flag_fileName_ecdc:
		thisDelimiter = flag_csvFile_ecdc_delimiter
		thisDecimal = flag_csvFile_ecdc_decimal
	
	if thisSourceFileFlag == flag_fileName_ecdcWeekly:
		thisDelimiter = flag_csvFile_ecdc_delimiter
		thisDecimal = flag_csvFile_ecdc_decimal
		
	if thisSourceFileFlag == flag_fileName_jhu:
		thisDelimiter = flag_csvFile_jhu_delimiter
		thisDecimal = flag_csvFile_jhu_decimal
	
	if thisSourceFileFlag == flag_fileName_RKI_District_Yesterday:
		thisDelimiter = flag_csvFile_rki_yesterday_delimiter
		thisDecimal = flag_csvFile_rki_yesterday_decimal
	
	if thisSourceFileFlag == flag_fileName_RKI_District_Timeline:
		thisDelimiter = flag_csvFile_rki_timeline_delimiter
		thisDecimal = flag_csvFile_rki_timeline_decimal
	
	# print(thisSourceFileFlag + " Delimiter: " + thisDelimiter)
	# print(thisSourceFileFlag + " Decimal: " + thisDecimal)
	
	return thisDelimiter, thisDecimal


# ######################################################################################################################
def func_prepareThisCovidData(
	df_thisCovidData,
	thisSourceFileFlag,
	flag_workInTogetherCMG
):
	print(chr(10) + " PREPARE " + thisSourceFileFlag + " dataset")
	print("### BEFORE DATASET PREP ###")
	func_printThisDataframeHeader(df_thisCovidData, 5, thisSourceFileFlag)
	
	if thisSourceFileFlag == flag_fileName_ecdc:
		df_thisCovidData = func_prepare_ecdcData(df_thisCovidData, dict_ecdc_dataset["old_daily"])
	
	# as of now this step is the same for daily and weekly
	if thisSourceFileFlag == flag_fileName_ecdcWeekly:
		df_thisCovidData = func_prepare_ecdcData(df_thisCovidData, dict_ecdc_dataset["new_weekly"])
		
	if thisSourceFileFlag == flag_fileName_jhu:
		df_thisCovidData = func_prepare_juhuData(df_thisCovidData)
		
	# func_exportPreparedCovidDataset(thisSourceFileFlag, df_thisCovidData, flag_workInTogetherCMG)
	
	print("### AFTER DATASET PREP ###")
	func_printThisDataframeHeader(df_thisCovidData, 5, thisSourceFileFlag)
	
	return df_thisCovidData


# ######################################################################################################################
def func_exportPreparedCovidDataset(
	thisSourceFileFlag,
	df_thisCovidData,
	flag_workInTogetherCMG
):
	currentDT = datetime.datetime.now()
	thisTimeNow = str(currentDT.strftime("%Y-%m-%d %h-%m"))
	
	thisDelimiter, thisDecimal = func_getCSVSeparationStrings(thisSourceFileFlag)
	
	localData_filePathAndName = dict_dataPaths_JHU[flag_workInTogetherCMG] + chr(
		92) + thisSourceFileFlag + "_PREPARED_confirmedCases_as_of_" + thisTimeNow + ".csv "
	
	print("export prepared " + thisSourceFileFlag + " dataset into " + localData_filePathAndName)
	
	func_exportThisDatasetIntoThisPathAndFile(df_thisCovidData, localData_filePathAndName, thisDelimiter, thisDecimal)


# ######################################################################################################################
def func_compareIR_inRelationToItalyPerDay(
	df_thisData,
	df_annex20CountryList,
	flag_ecdcDataset
):
	printAllDetailsInHere = False
	
	if flag_ecdcDataset == dict_ecdc_dataset["old_daily"]:
		harmonized_date = flag_ecdc_date
		harmonized_country = flag_ecdc_countryLongName
		harmonized_infectionRate = flag_ecdc_cumulative_IR_last_14_days
		harmonized_IR_comparedToItaly = flag_ecdc_IR_thisDayInRelationToItalysIR
	else:
		harmonized_date = flag_ecdcWeekly_date
		harmonized_country = flag_ecdcWeekly_countriesAndTerritories
		harmonized_infectionRate = flag_ecdcWeekly_IR14
		harmonized_IR_comparedToItaly = flag_ecdcWeekly_IR_thisDayInRelationToItalysIR

	func_replaceCountryNamesToHarmonizeWithAnnex20MasterList(df_thisData, harmonized_country)

	df_thisData[harmonized_IR_comparedToItaly] = -1
	
	for thisUniqueDay in df_thisData[harmonized_date].unique():
		df_dataAllCountriesThisDay = df_thisData[df_thisData[harmonized_date] == thisUniqueDay]
		
		subDF_ItalyThisDay = df_thisData[
			(df_thisData[harmonized_date] == thisUniqueDay) &
			(df_thisData[harmonized_country] == "Italy")
		]
		
		IR_ItalyThisDay = subDF_ItalyThisDay.loc[
			subDF_ItalyThisDay.index.max(),
			harmonized_infectionRate
		]
		
		print("IR_ItalyThisDay @ " + str(thisUniqueDay) + " = " + str(IR_ItalyThisDay))
		
		if math.isnan(float(IR_ItalyThisDay)) == False:
			for ap in df_dataAllCountriesThisDay.index:
				
				thisCountry = df_dataAllCountriesThisDay.loc[ap, harmonized_country]
				
				# if thisCountry in dict_oneMillionDifferentCountryNames:
				# 	thisCountry = dict_oneMillionDifferentCountryNames[thisCountry]
				# 	df_dataAllCountriesThisDay.loc[ap, harmonized_country] = \
				# 		thisCountry
				# 	# print("name mapping to: " + thisCountry)
				#
				if printAllDetailsInHere:
					print(chr(10) + "##################################### ")
					print("thisCountry (" + thisCountry + ")")
					
				if thisCountry == "Italy":
					if printAllDetailsInHere:
						print(" this is Italy, skip it, no need to compare Italy against Italy")
					
					df_thisData.loc[
						(df_thisData[harmonized_date] == thisUniqueDay) &
						(df_thisData[harmonized_country] == thisCountry),
						harmonized_IR_comparedToItaly
					] = 0
					
					continue
				if thisCountry in df_annex20CountryList.values:
					df_onlyThisCountry = df_annex20CountryList[df_annex20CountryList[flag_annex20_Country] == thisCountry]
					
					if printAllDetailsInHere:
						print("... START df_onlyThisCountry")
						print(df_onlyThisCountry)
						print("... END df_onlyThisCountry")
						
					annex20CountryFlag = df_onlyThisCountry.loc[
						df_onlyThisCountry.index.max(),
						flag_annex20_Annex20CountryLetter
					]
					
					if printAllDetailsInHere:
						print(thisCountry + " annex20CountryFlag: (" + annex20CountryFlag + ")")
					
					if annex20CountryFlag != "empty":
						if printAllDetailsInHere:
							print("skip this country ... Annex20 (" + df_annex20CountryList.loc[1, flag_annex20_Annex20CountryLetter] + ")")
						
						continue
					
					thisCountryInfectionRate = df_dataAllCountriesThisDay.loc[ap, harmonized_infectionRate]
					
					if printAllDetailsInHere:
						print(chr(10) + str(thisUniqueDay) + " CHECK IR FOR thisCountry: " + thisCountry + " IR: " + str(thisCountryInfectionRate))
					
					if math.isnan(
						float(
							df_dataAllCountriesThisDay.loc[ap, harmonized_infectionRate])
					) == False:
					
						if float(thisCountryInfectionRate) >= float(IR_ItalyThisDay):
							if printAllDetailsInHere:
								print(
									"IR WARNING FOR (" + thisCountry + ") >>> IR (" + str(thisCountryInfectionRate)+ ") >= IR-ITALY (" + str(IR_ItalyThisDay)+")"
								)
							
							df_thisData.loc[
								(df_thisData[harmonized_date] == thisUniqueDay) &
								(df_thisData[harmonized_country] == thisCountry),
								harmonized_IR_comparedToItaly
							] = 1
						else:
							if printAllDetailsInHere:
								print(
									"ALL FINE @ IR FOR (" + thisCountry + ") >>> IR (" + str(
										thisCountryInfectionRate) + ") < IR-ITALY (" + str(IR_ItalyThisDay) + ")"
								)
							
							df_thisData.loc[
								(df_thisData[harmonized_date] == thisUniqueDay) &
								(df_thisData[harmonized_country] == thisCountry),
								harmonized_IR_comparedToItaly
							] = 0
					else:
						if printAllDetailsInHere:
							print("ATTENTION! NO IR FOR (" + thisCountry + ") at this day ... IR ("+str(thisCountryInfectionRate)+")")
				else:
					print(
						"ATTENTION!!! NEW COUNTRY in ECDC Country List ... PLEASE ADD " + thisCountry + " into the ANNEX 20 XLS File!!")
		
	return df_thisData


# ######################################################################################################################
def func_prepare_juhuData(
	df_thisCovidData
):
	df_thisCovidData = func_replaceNanInThisColumn(df_thisCovidData, flag_jhuSourceFile_Province, "")
	
	df2_transposed = df_thisCovidData.transpose()
	
	df2_transposed = df2_transposed.reset_index(drop=False)
	
	func_printThisDataframeHeader(df2_transposed, 5)
	
	df2_transposed = func_eraseRowWithThatContentInThisColumn(df2_transposed, "Lat", "index")
	
	df2_transposed = func_eraseRowWithThatContentInThisColumn(df2_transposed, "Long", "index")
	
	df2_transposed = func_renameColumnsWithCountrynames(df2_transposed)
	
	df2_transposed = func_eraseRowWithThatContentInThisColumn(df2_transposed, "Province/State", "Country/Region_Province/State")
	
	df2_transposed = func_eraseRowWithThatContentInThisColumn(df2_transposed, "Country/Region", "Country/Region_Province/State")
	
	df2_transposed = df2_transposed.rename(columns={"Country/Region_Province/State": flag_jhu_converted_date})
	
	df2_transposed[flag_jhu_converted_date] = pd.to_datetime(df2_transposed[flag_jhu_converted_date])
	
	df2_finalFile = func_createUsefulStructureOutOfJHU(df2_transposed)
	
	df2_finalFile = func_calculate_IR_comparedToItaly(df2_finalFile)
	
	return df2_finalFile


# ######################################################################################################################
def func_calculate_IR_comparedToItaly(
	dfInput
):
	# <>
	print("Final Step: IR compared to Italys IR")
	
	# dfInput[flag_jhu_converted_IR7_above_or_belowItaly] = dict_above_or_belowIR_Italy["notCalculatedYet"]
	dfInput[flag_jhu_converted_IR14_above_or_belowItaly] = dict_above_or_belowIR_Italy["notCalculatedYet"]
	dfInput[flag_jhu_converted_IR14_DeltaPreviousDay] = 0
	
	for ap in dfInput.index:
		if dfInput.loc[ap, flag_jhu_converted_country] == "Italy":
			# dfInput.loc[flag_jhu_converted_IR7_above_or_belowItaly] = dict_above_or_belowIR_Italy["below_IR_Italy"]
			dfInput.loc[ap, flag_jhu_converted_IR14_above_or_belowItaly] = dict_above_or_belowIR_Italy["below_IR_Italy"]
			continue
		
		thisCountryThisDayIR14 = dfInput.loc[ap, flag_jhu_converted_cumulative_IR_14days]
		IR14_ItalyThisDay = dfInput[flag_jhu_converted_cumulative_IR_14days][
			(dfInput[flag_jhu_converted_country] == "Italy") &
			(dfInput[flag_jhu_converted_date] == dfInput.loc[ap, flag_jhu_converted_date])].values[0]
		
		# print("##########################")
		# print("this day: " + str(dfInput.loc[ap, flag_jhu_converted_country]))
		# print("this day: " + str(dfInput.loc[ap, flag_jhu_converted_date]))
		# print("thisCountryThisDayIR14 " + str(thisCountryThisDayIR14))
		# print("IR14_ItalyThisDay " + str(IR14_ItalyThisDay))
		#
		if thisCountryThisDayIR14 <= IR14_ItalyThisDay:
			# print("below")
			dfInput.loc[ap, flag_jhu_converted_IR14_above_or_belowItaly] = dict_above_or_belowIR_Italy["below_IR_Italy"]
		else:
			dfInput.loc[ap, flag_jhu_converted_IR14_above_or_belowItaly] = dict_above_or_belowIR_Italy["above_IR_Italy"]
			# print("above")
	
	for ap in dfInput.index:
		if ap > 0:
			if dfInput.loc[ap, flag_jhu_converted_country] == dfInput.loc[ap-1, flag_jhu_converted_country]:
				dfInput.loc[ap, flag_jhu_converted_IR14_DeltaPreviousDay] = \
					dfInput.loc[ap, flag_jhu_converted_cumulative_IR_14days] - \
					dfInput.loc[ap - 1, flag_jhu_converted_cumulative_IR_14days]
	
	return dfInput

	
# ######################################################################################################################
def func_createUsefulStructureOutOfJHU(
	df_jhu
):
	# flag_jhu_converted_date = "Date"
	# flag_jhu_converted_country = "Country"
	# flag_jhu_converted_totalCasesUntilThisDay = "total cases"
	# flag_jhu_converted_deltaPreviousDay = "new cases"
	# flag_jhu_converted_cumulative_IR_7days = "IR 7 days"
	# flag_jhu_converted_cumulative_IR_14days = "IR 14 days"
	
	df_new = pd.DataFrame()
	ap_dfNew = -1
	
	for thisCountry in df_jhu.columns:
		if thisCountry == flag_jhu_converted_date:
			continue
		
		print("##########################")
		
		if thisCountry not in df_annex20CountryList.values:
			print(thisCountry + " skip this country, it is not in annex 20 country list ")
			continue
		
		# annex20CountryListLetter = df_annex20CountryList.loc[
		# 	df_annex20CountryList[flag_annex20_Country] == thisCountry,
		# 	flag_annex20_Annex20CountryLetter
		# ]
		
		# annex20CountryListLetter = df_annex20CountryList[
		# 	df_annex20CountryList[flag_annex20_Country] == thisCountry
		# ][flag_annex20_Annex20CountryLetter]
		
		annex20CountryListLetter = \
			df_annex20CountryList[flag_annex20_Annex20CountryLetter][df_annex20CountryList[flag_annex20_Country] == thisCountry].values[0]
		
		if len(annex20CountryListLetter) == 1 and thisCountry != "United Arab Emirates":
			print("SKIP " + thisCountry + " ANNEX 20 Letter: " + annex20CountryListLetter + " ... no Infection-Rate needed, Country on black-list")
			continue
		
		totalPopulation = func_getPopulationAsPerEurostatOrWorldBank(thisCountry)
		
		if totalPopulation == 0:
			print("SKIP " + thisCountry + " NO POPULATION data available")
			continue
		
		print(thisCountry + " ... prepare JHU data structure!")
		
		ap_thisCountry = 0
		for ap in df_jhu.index:
			ap_dfNew += 1
			ap_thisCountry += 1
			
			# print("date has to be changed from " + str(df_jhu.loc[ap, flag_jhu_converted_date]) + " to " + str(df_jhu.loc[ap, flag_jhu_converted_date] + timedelta(days=1)))
			
			df_new.loc[ap_dfNew, flag_jhu_converted_date] = df_jhu.loc[ap, flag_jhu_converted_date] + timedelta(days=1)
			df_new.loc[ap_dfNew, flag_jhu_converted_country] = thisCountry
			df_new.loc[ap_dfNew, flag_jhu_converted_totalCasesUntilThisDay] = df_jhu.loc[ap, thisCountry]
			
			if ap_thisCountry > 1:
				df_new.loc[ap_dfNew, flag_jhu_converted_deltaPreviousDay] = \
					df_new.loc[ap_dfNew, flag_jhu_converted_totalCasesUntilThisDay] - \
					df_new.loc[ap_dfNew - 1, flag_jhu_converted_totalCasesUntilThisDay]
			
			if totalPopulation != 0:
				if ap_thisCountry >= 7:
					df_new.loc[ap_dfNew, flag_jhu_converted_cumulative_IR_7days] = \
						(
							df_new.loc[ap_dfNew-6:ap_dfNew, flag_jhu_converted_deltaPreviousDay].sum() /
							totalPopulation
						) * 100000
				
				if ap_thisCountry >= 14:
					df_new.loc[ap_dfNew, flag_jhu_converted_cumulative_IR_14days] = \
						(
							df_new.loc[ap_dfNew - 13:ap_dfNew, flag_jhu_converted_deltaPreviousDay].sum() /
							totalPopulation
						) * 100000
					
	return df_new


# ######################################################################################################################
def func_getPopulationAsPerEurostatOrWorldBank(
	thisCountry
):
	totalPop = 0
	
	# if thisCountry in df_unitedNations.values:
	# 	totalPop = round(
	# 		float(
	# 			df_unitedNations[df_unitedNations[flag_unPop_Location] == thisCountry][flag_unPop_PopTotal]) * 1000, 0
	# 	)
	# 	print("totalPopulation in 2019 " + str(totalPop))
	# else:
	# 	print("This Country does not exist (or is written different) in the UN Population list")
	
	if thisCountry in df_eurostatPopulation.values:
		totalPop = round(
			float(
				df_eurostatPopulation[
					df_eurostatPopulation[flag_eurostat_CountryLabel] == thisCountry][flag_eurostat_population]), 0
		)
		print("EUROSTAT totalPopulation in 2020 " + str(totalPop))
	else:
		if thisCountry in df_worldBankPopulation.values:
			totalPop = round(
				float(
					df_worldBankPopulation[
						df_worldBankPopulation[flag_WorldBank_CountryName] == thisCountry][flag_worldBank_Population_EndOf2019]), 0
			)
			print("World-Bank totalPopulation in 2020 " + str(totalPop))
		else:
			print("This Country does not exist (or is written different) in the Eurostat Country list")
		
	return totalPop
	
	
# ######################################################################################################################
def func_eraseRowWithThatContentInThisColumn(
	df2_transposed,
	thisContent,
	columnName
):
	for ap in df2_transposed.index:
		if df2_transposed.loc[ap, columnName] == thisContent:
			print("erase " + df2_transposed.loc[ap, columnName] + " @ index " + str(ap))
			
			df2_transposed = df2_transposed.drop(df2_transposed.index[ap])
			
			func_printThisDataframeHeader(df2_transposed, 5)
			
			break
	
	df2_transposed = df2_transposed.reset_index(drop=True)
	
	return df2_transposed


# ######################################################################################################################
def func_renameColumnsWithCountrynames(
	df2_transposed
):
	
	for thisColumn in df2_transposed.columns:
		if len(str(df2_transposed.loc[0, thisColumn])) > 0:
			newColumnName = str(df2_transposed.loc[1, thisColumn]) + "_" + str(df2_transposed.loc[0, thisColumn])
		else:
			newColumnName = str(df2_transposed.loc[1, thisColumn])

		# print(df2_transposed.loc[0, thisColumn])
		print("newColumnName: " + str(newColumnName))

		if newColumnName in dict_jhuCountryNamesToBeUpdatedBeforeDataPreparation:
			print("Change this columnname from >" +
				  newColumnName + "< to " +
				  dict_jhuCountryNamesToBeUpdatedBeforeDataPreparation[newColumnName]
				  )
			newColumnName = dict_jhuCountryNamesToBeUpdatedBeforeDataPreparation[newColumnName]

		df2_transposed = df2_transposed.rename(columns={thisColumn: newColumnName})

	return df2_transposed


# ######################################################################################################################
def func_replaceNanInThisColumn(
	dfInput,
	columnFlag,
	newNanValue
):
	dfInput[columnFlag].fillna(newNanValue, inplace=True)
	
	return dfInput

# ######################################################################################################################
def func_prepare_ecdcData(
	df_thisCovidData,
	flag_ecdcDataset
):
	df_thisCovidData[flag_ecdc_date] = pd.to_datetime(df_thisCovidData[flag_ecdc_date], format='%d/%m/%Y')
	
	if flag_ecdcDataset == dict_ecdc_dataset["old_daily"]:
		df_thisCovidData = df_thisCovidData.sort_values([flag_ecdc_countryLongName, flag_ecdc_date],
																		ascending=(True, True))
	else:
		df_thisCovidData = df_thisCovidData.sort_values([flag_ecdcWeekly_countriesAndTerritories, flag_ecdcWeekly_date],
																		ascending=(True, True))
		
	df_thisCovidData = df_thisCovidData.reset_index(drop=True)
	
	# for thisColumn in df_thisCovidData.columns:
	# 	print("ECDC data column in raw data: " + thisColumn)
		
	df_thisCovidData = func_addCumulativeTotalPerCountry(df_thisCovidData, flag_ecdcDataset)
	
	if flag_ecdcDataset == dict_ecdc_dataset["old_daily"]:
		df_thisCovidData = func_addInfectionRateLast7Days(df_thisCovidData)
		
	return df_thisCovidData


# ######################################################################################################################
def func_addInfectionRateLast7Days(
	df_thisCovidData
):
	df_thisCovidData[flag_ecdc_cumulative_IR_last_7_days] = 0
	df_thisCovidData[flag_ecdc_totalNewCases_last_7_days] = 0
	df_thisCovidData[flag_ecdc_totalNewCases_last_14_days] = 0
	nextCountryname = ""
	
	if flag_calculatedIR7ForECDC:
		for ap in df_thisCovidData.index:
			if ap >= 7:
				if df_thisCovidData.loc[ap, flag_ecdc_countryLongName] == df_thisCovidData.loc[ap-6, flag_ecdc_countryLongName]:
					totalCasesThisPeriodThisCountry = 0
					totalCasesThisPeriodThisCountry = df_thisCovidData.loc[ap-6: ap, flag_ecdc_newCasesThisDay].sum()
					
					df_thisCovidData.loc[ap, flag_ecdc_totalNewCases_last_7_days] = totalCasesThisPeriodThisCountry
					
					if ap >= 14:
						if df_thisCovidData.loc[ap, flag_ecdc_countryLongName] == \
							df_thisCovidData.loc[ap - 13, flag_ecdc_countryLongName]:
							df_thisCovidData.loc[ap, flag_ecdc_totalNewCases_last_14_days] = df_thisCovidData.loc[ap-13: ap, flag_ecdc_newCasesThisDay].sum()
						
					if totalCasesThisPeriodThisCountry > 0:
						ir7DayRollingECDC = \
							round(
								totalCasesThisPeriodThisCountry / float(df_thisCovidData.loc[ap, flag_ecdc_officialPopulationDataIn_2019]) *100000, 1)
					
						df_thisCovidData.loc[ap, flag_ecdc_cumulative_IR_last_7_days] = ir7DayRollingECDC
						
				else:
					if \
						df_thisCovidData.loc[ap, flag_ecdc_countryLongName] != \
						df_thisCovidData.loc[ap - 1, flag_ecdc_countryLongName]:
						nextCountryname = df_thisCovidData.loc[ap, flag_ecdc_countryLongName]
						print("ECDC IR 7 Statistics for " + nextCountryname)
					
	return df_thisCovidData


# ######################################################################################################################
def func_addCumulativeTotalPerCountry(
	df_thisCovidData,
	flag_ecdcDataset
):
	if flag_ecdcDataset == dict_ecdc_dataset["old_daily"]:
		df_thisCovidData[flag_ecdc_cumulativeTotalCases] = 0
		
		for ap in df_thisCovidData.index:
			if ap > 0:
				if df_thisCovidData.loc[ap, flag_ecdc_countryLongName] == \
					df_thisCovidData.loc[ap - 1, flag_ecdc_countryLongName]:
					
					df_thisCovidData.loc[ap, flag_ecdc_cumulativeTotalCases] = \
						df_thisCovidData.loc[ap - 1 , flag_ecdc_cumulativeTotalCases] + \
						df_thisCovidData.loc[ap, flag_ecdc_newCasesThisDay]
	
	if flag_ecdcDataset == dict_ecdc_dataset["new_weekly"]:
		df_thisCovidData[flag_ecdcWeekly_cumulativeTotalCases] = 0
		
		for ap in df_thisCovidData.index:
			if ap > 0:
				if df_thisCovidData.loc[ap, flag_ecdcWeekly_countriesAndTerritories] == \
					df_thisCovidData.loc[ap - 1, flag_ecdcWeekly_countriesAndTerritories]:
					
					df_thisCovidData.loc[ap, flag_ecdcWeekly_cumulativeTotalCases] = \
						df_thisCovidData.loc[ap - 1, flag_ecdcWeekly_cumulativeTotalCases] + \
						df_thisCovidData.loc[ap, flag_ecdcWeekly_cases_weekly]
	
	return df_thisCovidData


# ######################################################################################################################
def func_getPathAndFileNameForLocalDataStorage(
	thisSourceFileFlag,
	flag_thisPathWay,
	flag_raw_vs_prepared,
	subFileWithTimeOrMaster
):
	localData_filePathAndName = ""
	
	currentDT = datetime.datetime.now()
	thisTimeNow = str(currentDT.strftime("%Y-%m-%d %H-%M"))
	
	fileNameAppendix = ""
	
	if subFileWithTimeOrMaster == dict_masterFileOrSubfileWithTimeStamp["fileType_dailyWithTime"]:
		fileNameAppendix = chr(92) + thisSourceFileFlag + "_" + flag_raw_vs_prepared + "_as_of_" + thisTimeNow + ".csv "
	
	if subFileWithTimeOrMaster == dict_masterFileOrSubfileWithTimeStamp["fileType_master"]:
		fileNameAppendix = chr(92) + "_MASTER_" + thisSourceFileFlag + "_" + flag_raw_vs_prepared + ".csv "
		
	if thisSourceFileFlag == flag_fileName_ecdc:
		localData_filePathAndName = dict_dataPaths_ECDC[flag_thisPathWay] + fileNameAppendix
		print(chr(10) + thisSourceFileFlag + " backup of " + flag_raw_vs_prepared + " file saved in ... ")
		print(localData_filePathAndName)
	
	if thisSourceFileFlag == flag_fileName_ecdcWeekly:
		localData_filePathAndName = dict_dataPaths_ECDC[flag_thisPathWay] + fileNameAppendix
		print(chr(10) + thisSourceFileFlag + " backup of " + flag_raw_vs_prepared + " file saved in ... ")
		print(localData_filePathAndName)
		
	if thisSourceFileFlag == flag_fileName_jhu:
		localData_filePathAndName = dict_dataPaths_JHU[flag_thisPathWay] + fileNameAppendix
		print(chr(10) + thisSourceFileFlag + " backup of " + flag_raw_vs_prepared + " file saved in ... ")
		print(localData_filePathAndName)
	
	if thisSourceFileFlag == flag_fileName_RKI_District_Yesterday:
		localData_filePathAndName = dict_dataPaths_RKI_districtYesterday[flag_thisPathWay] + fileNameAppendix
		print(chr(10) + thisSourceFileFlag + " backup of " + flag_raw_vs_prepared + " file saved in ... ")
		print(localData_filePathAndName)
	
	if thisSourceFileFlag == flag_fileName_RKI_District_Timeline:
		localData_filePathAndName = dict_dataPaths_RKI_districtTimeline[flag_thisPathWay] + fileNameAppendix
		print(chr(10) + thisSourceFileFlag + " backup of " + flag_raw_vs_prepared + " file saved in ... ")
		print(localData_filePathAndName)
		
	if len(localData_filePathAndName) == 0:
		print("NO LOCAL SOURCE PATH FOUND FOR: " + thisSourceFileFlag)
	
	return localData_filePathAndName


# ######################################################################################################################
def func_getSourceDataPath(
	thisSourceFileFlag
):
	if thisSourceFileFlag == flag_fileName_ecdc:
		print(chr(10) + thisSourceFileFlag + " source data download from: " + flag_ecdc_sourceDataPathFromTheInternet)
		return flag_ecdc_sourceDataPathFromTheInternet
	
	if thisSourceFileFlag == flag_fileName_ecdcWeekly:
		print(chr(10) + thisSourceFileFlag + " source data download from: " + flag_ecdcWeekly_sourceDataPathFromTheInternet)
		return flag_ecdcWeekly_sourceDataPathFromTheInternet
	
	if thisSourceFileFlag == flag_fileName_jhu:
		print(chr(10) + thisSourceFileFlag + " source data download from: " + flag_jhu_sourceDataPathFromTheInternet)
		return flag_jhu_sourceDataPathFromTheInternet
	
	print("NO INTERNET SOURCE PATH FOUND FOR: " + thisSourceFileFlag)
	return ""


# ######################################################################################################################
def func_exportThisDatasetIntoThisPathAndFile(
	dfToBeExported,
	pathAndFileName,
	thisDelimiter,
	thisDecimal
):
	dfToBeExported.to_csv(
		pathAndFileName,
		sep=masterDelimiterFinalFiles,
		decimal=thisDecimal,
		index=False
	)
	

# ######################################################################################################################
def func_exportFinalFileIntoPBIFolder(
	df_thisCovidData,
	thisSourceFileFlag,
	flag_workInTogetherCMG
):
	thisDelimiter, thisDecimal = func_getCSVSeparationStrings(thisSourceFileFlag)
	localData_filePathAndName = func_getPathAndFileNameForFinalPBIFile(thisSourceFileFlag, flag_workInTogetherCMG)
	
	func_exportThisDatasetIntoThisPathAndFile(df_thisCovidData, localData_filePathAndName, thisDelimiter, thisDecimal)


# ######################################################################################################################
def func_getPathAndFileNameForFinalPBIFile(
	thisSourceFileFlag,
	flag_workInTogetherCMG
):
	localData_filePathAndName = ""
	
	if thisSourceFileFlag == flag_fileName_ecdc:
		localData_filePathAndName = \
			dict_flag_pbi_path_finalFolder[flag_workInTogetherCMG] + chr(92) + flag_pbi_name_fileName_ECDC + ".csv "
		
		print(chr(10) + thisSourceFileFlag + " final pbi file will be saved here ... ")
		print(localData_filePathAndName)
	
	if thisSourceFileFlag == flag_fileName_ecdcWeekly:
		localData_filePathAndName = \
			dict_flag_pbi_path_finalFolder[flag_workInTogetherCMG] + chr(92) + flag_pbi_name_fileName_ECDC_Weekly + ".csv "
		
		print(chr(10) + thisSourceFileFlag + " final pbi file will be saved here ... ")
		print(localData_filePathAndName)
		
	if thisSourceFileFlag == flag_fileName_jhu:
		localData_filePathAndName = \
			dict_flag_pbi_path_finalFolder[flag_workInTogetherCMG] + chr(92) + flag_pbi_name_fileName_JHU + ".csv "
		
		print(chr(10) + thisSourceFileFlag + " final pbi file will be saved here ... ")
		print(localData_filePathAndName)
	
	if thisSourceFileFlag == flag_fileName_RKI_District_Yesterday:
		localData_filePathAndName = \
			dict_flag_pbi_path_finalFolder[flag_workInTogetherCMG] + chr(92) + flag_pbi_name_fileName_RKI_District_Yesterday + ".csv "
		
		print(chr(10) + thisSourceFileFlag + " final pbi file will be saved here ... ")
		print(localData_filePathAndName)
	
	if thisSourceFileFlag == flag_fileName_RKI_District_Timeline:
		localData_filePathAndName = \
			dict_flag_pbi_path_finalFolder[flag_workInTogetherCMG] + chr(92) + flag_pbi_name_fileName_RKI_District_Timeline + ".csv "
		
		print(chr(10) + thisSourceFileFlag + " final pbi file will be saved here ... ")
		print(localData_filePathAndName)
	
	if len(localData_filePathAndName) == 0:
		print("NO LOCAL SOURCE PATH FOUND FOR: " + thisSourceFileFlag)
	
	return localData_filePathAndName


# ######################################################################################################################
def func_readDataFromRKI_via_NPGEO_InJson():
	response = requests.request("GET", url_districtsYesterday)
	thisResult = response.json()

	print(thisResult)

	df_districts = pd.DataFrame()
	
	cnt = 0
	
	for thisFeatures in thisResult["features"]:
		print("RKI-IR7 for district " + thisFeatures["attributes"]["GEN"] + ": " + str(thisFeatures["attributes"]["cases7_per_100k"]))
		
		df_districts.loc[cnt, flag_rki_districtName] = \
			thisFeatures["attributes"][dict_featureNames["rki_districtName"]] #+ " (" + \
			#thisFeatures["attributes"][dict_featureNames["rki_districtBEZ"]] + ")"

		df_districts.loc[cnt, flag_rki_districtStadtLandkreis] = \
			thisFeatures["attributes"][dict_featureNames["rki_districtBEZ"]]
		
		df_districts.loc[cnt, flag_rki_casesPer100k] = \
			thisFeatures["attributes"][dict_featureNames["rki_casesPer100k"]]
		
		df_districts.loc[cnt, flag_rki_cases7Per100k] = \
			thisFeatures["attributes"][dict_featureNames["rki_IR7per100k"]]
		
		df_districts.loc[cnt, flag_rki_districtPopulation] = \
			thisFeatures["attributes"][dict_featureNames["rki_districtPopulation"]]
		
		df_districts.loc[cnt, flag_rki_totalCases] = \
			thisFeatures["attributes"][dict_featureNames["rki_totalCases"]]
		
		df_districts.loc[cnt, flag_rki_lastUpdate] = \
			thisFeatures["attributes"][dict_featureNames["rki_lastUpdateDate"]]
		
		df_districts.loc[cnt, flag_rki_agsNumber] = \
			thisFeatures["attributes"][dict_featureNames["rki_AGS_Number"]]
		
		cnt += 1
	
	
	return(df_districts)


# ######################################################################################################################
def func_exportThisFileIntoThisFolder(
	df_thisDataset,
	thisSourceFileFlag,
	flag_thisPathWay,
	flag_raw_vs_prepared,
	subFileWithTimeOrMaster
):
	thisDelimiter, thisDecimal = func_getCSVSeparationStrings(thisSourceFileFlag)
	localData_filePathAndName = func_getPathAndFileNameForLocalDataStorage(
		thisSourceFileFlag, flag_thisPathWay, flag_raw_vs_prepared, subFileWithTimeOrMaster
	)
	
	func_exportThisDatasetIntoThisPathAndFile(
		df_thisDataset, localData_filePathAndName, thisDelimiter, thisDecimal
	)


# ######################################################################################################################
def func_getPathToSaveFiles(
	username
):
	flag_workInTogetherCMG = False

	if username != 'TR@FI_02':
		print(chr(10) + "### WORKING & SAVING FILES IN SHARED DRIVE Together_CMG\Covid19_ECDC_JHU? (yes or no?)")
		choice = input().lower()
		if choice in yes:
			print(">> YES >> all data will be saved in shared drive \wrfile11\cmg\Together_CMG\Covid19_ECDC_JHU")
			flag_workInTogetherCMG = True
			flag_workInTogetherCMG = dict_possiblePaths["path_togetherCMG"]
		elif choice in no:
			print(">> NO >> all data will be saved in C:\COVID_Reporting")
			# flag_workInTogetherCMG = False
			flag_workInTogetherCMG = dict_possiblePaths["path_local"]
		else:
			sys.stdout.write("Please respond with 'yes' or 'no' >>> TOOL STOPPED")
			exit()
	else:
		print("This user has no access into CMG network, work in local environment")
		# flag_workInTogetherCMG = False
		flag_workInTogetherCMG = dict_possiblePaths["path_TR_HomeLocal"]

	return flag_workInTogetherCMG

# ######################################################################################################################
def func_saveCopyInCostaGroupSharedDrive(
	username
):
	savedCopyInGroupShare = False

	if username != 'TR@FI_02':
		print(chr(10) + "### save copy of data in Costa Group Shared Drive (costafs.costa.it\groupshare\Public\Covid19_ECDC_JHU\Data) (yes or no?)")
		choice = input().lower()
		if choice in yes:
			print(">> YES >> copy of data will be saved in group shared drive")
			savedCopyInGroupShare = True
		elif choice in no:
			print(">> NO >> no copy in costa group shared drive")
			savedCopyInGroupShare = False
		else:
			sys.stdout.write("Please respond with 'yes' or 'no' >>> TOOL STOPPED")
			exit()
	else:
		print(username + " has no access to CMG shared drive ... do not save any data in Costa Group Share")

	return savedCopyInGroupShare


# ######################################################################################################################
def func_getKeyForDataStorageLocalVsShared(
	flag_workInTogetherCMG
):
	finalKey = 0
	
	if flag_workInTogetherCMG == 0:
		finalKey = dict_possiblePaths["path_local"]
	
	if flag_workInTogetherCMG == 1:
		finalKey = dict_possiblePaths["path_togetherCMG"]

	if flag_workInTogetherCMG == 3:
		finalKey = dict_possiblePaths["path_TR_HomeLocal"]

	return finalKey


# ######################################################################################################################
def func_doAllAroundSavingThisSourceDataset(
	df_thisDataset,
	flag_Datasource,
	flag_raw_vs_prepared,
	flag_workInTogetherCMG,
	flag_saveCopyInCostaGroupSharedDrive
):
	keyPathDict = flag_workInTogetherCMG #func_getKeyForDataStorageLocalVsShared(flag_workInTogetherCMG)
	func_exportThisFileIntoThisFolder(
		df_thisDataset, flag_Datasource, keyPathDict,
		flag_raw_vs_prepared, dict_masterFileOrSubfileWithTimeStamp["fileType_dailyWithTime"]
	)

	if flag_saveCopyInCostaGroupSharedDrive:
		func_exportThisFileIntoThisFolder(
			df_thisDataset, flag_Datasource, dict_possiblePaths["path_costaGroupShared"],
			flag_raw_vs_prepared, dict_masterFileOrSubfileWithTimeStamp["fileType_dailyWithTime"]
		)
		
		func_exportThisFileIntoThisFolder(
			df_thisDataset, flag_Datasource, dict_possiblePaths["path_costaGroupShared"],
			flag_raw_vs_prepared, dict_masterFileOrSubfileWithTimeStamp["fileType_master"]
		)
		

# ######################################################################################################################
def func_readAnnex20CountryList(
	flag_workInTogetherCMG
):
	xlsFileHandle = pd.ExcelFile(dict_dataPaths_annex20CountryList[flag_workInTogetherCMG])
	
	df_annex20CountryList = pd.read_excel(
		xlsFileHandle,
		sheet_name="Country_List",
		dtype=str,
		skiprows=0
	)
	
	df_annex20CountryList = func_replaceNanInThisColumn(df_annex20CountryList, flag_annex20_Annex20CountryLetter, "empty")
	
	df_annex20CountryList = df_annex20CountryList.astype(str)
	
	print("##################################")
	for ap in df_annex20CountryList.index:
		print("| " + str(ap) + " = " + df_annex20CountryList.loc[ap, flag_annex20_Country] + " ANNEX20 (" + df_annex20CountryList.loc[ap, flag_annex20_Annex20CountryLetter] + ") |")
		if df_annex20CountryList.loc[ap, flag_annex20_Annex20CountryLetter] == "C":
			print("JEA, its a C")
		if df_annex20CountryList.loc[ap, flag_annex20_Annex20CountryLetter] == "D":
			print("JEA, its a D")
		if df_annex20CountryList.loc[ap, flag_annex20_Annex20CountryLetter] == "empty":
			print("OH NO, its empty")
		
	print("##################################")
	# print(df_annex20CountryList.head(8))
	
	for thisColumn in df_annex20CountryList.columns:
		print("oh yea, next column: " + thisColumn)
	
	func_printThisDataframeHeader(df_annex20CountryList, 5)
	
	return df_annex20CountryList


# ######################################################################################################################
def func_replaceNanInThisColumn(
	dfInput,
	columnFlag,
	newNanValue
):
	dfInput[columnFlag].fillna(newNanValue, inplace=True)
	
	return dfInput


# ######################################################################################################################
def func_fill_IR_DeltaFigures(
	df_thisDataset
):
	df_thisDataset[flag_ecdc_IR14_deltaPerDay] = 0
	df_thisDataset[flag_ecdc_IR14_avgDeltaLastWeek] = 0
	df_thisDataset[flag_ecdc_timeLeftBeforeCrossingItaly] = -9999

	for ap in df_thisDataset.index:
		if ap == 0:
			continue
			
		if \
			df_thisDataset.loc[ap, flag_ecdc_countryLongName] == df_thisDataset.loc[ap-1, flag_ecdc_countryLongName]:
			df_thisDataset.loc[ap, flag_ecdc_IR14_deltaPerDay] = \
				df_thisDataset.loc[ap, flag_ecdc_cumulative_IR_last_14_days] - \
				df_thisDataset.loc[ap-1, flag_ecdc_cumulative_IR_last_14_days]
		
		if ap > 7:
			if \
				df_thisDataset.loc[ap, flag_ecdc_countryLongName] == df_thisDataset.loc[ap-6, flag_ecdc_countryLongName]:
				df_thisDataset.loc[ap, flag_ecdc_IR14_avgDeltaLastWeek] = \
					df_thisDataset.loc[ap-6:ap, flag_ecdc_IR14_deltaPerDay].mean()
				
			
	return df_thisDataset


# ######################################################################################################################
def func_calculateIRPredictionForCrossingItaly(
	df_thisData,
	df_annex20CountryList
):
	printAllDetailsInHere = False
	
	for thisUniqueDay in df_thisData[flag_ecdc_date].unique():
		df_dataAllCountriesThisDay = df_thisData[df_thisData[flag_ecdc_date] == thisUniqueDay]
		
		subDF_ItalyThisDay = df_thisData[
			(df_thisData[flag_ecdc_date] == thisUniqueDay) &
			(df_thisData[flag_ecdc_countryLongName] == "Italy")
			]
		
		IR_Absolut_ItalyThisDay = subDF_ItalyThisDay.loc[
			subDF_ItalyThisDay.index.max(),
			flag_ecdc_cumulative_IR_last_14_days
		]
		
		IR_avgDeltaLastWeek_ItalyThisDay = subDF_ItalyThisDay.loc[
			subDF_ItalyThisDay.index.max(),
			flag_ecdc_IR14_avgDeltaLastWeek
		]
		
		print(
			str(thisUniqueDay) +
			" = IR ITALY (" + str(IR_Absolut_ItalyThisDay) + ")" + " IR DELTA AVG LAST WEEK (" + str(IR_avgDeltaLastWeek_ItalyThisDay) + ")"
		)
		
		if math.isnan(float(IR_Absolut_ItalyThisDay)) == False:
			for ap in df_dataAllCountriesThisDay.index:
				
				thisCountry = df_dataAllCountriesThisDay.loc[ap, flag_ecdc_countryLongName]
				
				if thisCountry in df_annex20CountryList.values:
					if printAllDetailsInHere:
						print(chr(10) + "##################################### ")
						print("thisCountry (" + thisCountry + ")")
					
					if thisCountry == "Italy":
						if printAllDetailsInHere:
							print(" this is Italy, skip it, no need to compare Italy against Italy")
						
						df_thisData.loc[
							(df_thisData[flag_ecdc_date] == thisUniqueDay) &
							(df_thisData[flag_ecdc_countryLongName] == thisCountry),
							flag_ecdc_timeLeftBeforeCrossingItaly
						] = 0
						
						continue
					
					df_onlyThisCountry = df_annex20CountryList[df_annex20CountryList[flag_annex20_Country] == thisCountry]
					
					if printAllDetailsInHere:
						print("... START df_onlyThisCountry")
						print(df_onlyThisCountry)
						print("... END df_onlyThisCountry")
					
					annex20CountryFlag = df_onlyThisCountry.loc[
						df_onlyThisCountry.index.max(),
						flag_annex20_Annex20CountryLetter
					]
					
					if printAllDetailsInHere:
						print(thisCountry + " annex20CountryFlag: (" + annex20CountryFlag + ")")
					
					if annex20CountryFlag != "empty":
						if printAllDetailsInHere:
							print("skip this country ... Annex20 (" + df_annex20CountryList.loc[
								1, flag_annex20_Annex20CountryLetter] + ")")
						
						continue
					
					IR_Absolut_ThisCountryThisDay = df_dataAllCountriesThisDay.loc[ap, flag_ecdc_cumulative_IR_last_14_days]
					IR_avgDeltaLastWeek_ThisCountryThisDay = df_dataAllCountriesThisDay.loc[ap, flag_ecdc_IR14_avgDeltaLastWeek]
					
					if \
						(
							IR_Absolut_ThisCountryThisDay < IR_Absolut_ItalyThisDay and
							IR_avgDeltaLastWeek_ThisCountryThisDay > IR_avgDeltaLastWeek_ItalyThisDay
						) \
						or \
						(
							IR_Absolut_ThisCountryThisDay > IR_Absolut_ItalyThisDay and
							IR_avgDeltaLastWeek_ThisCountryThisDay < IR_avgDeltaLastWeek_ItalyThisDay
						):
						
						daysTillCrossing = round(
							(IR_Absolut_ThisCountryThisDay - IR_Absolut_ItalyThisDay) / (IR_avgDeltaLastWeek_ItalyThisDay - IR_avgDeltaLastWeek_ThisCountryThisDay), 0)
						
						print("daysTillCrossing for " + thisCountry + " = ("+str(daysTillCrossing)+")")
						
						if daysTillCrossing >= -7 and daysTillCrossing <= 7:
							
							if IR_Absolut_ThisCountryThisDay > IR_Absolut_ItalyThisDay:
								daysTillCrossing = daysTillCrossing * -1
							
							df_thisData.loc[
								(df_thisData[flag_ecdc_date] == thisUniqueDay) &
								(df_thisData[flag_ecdc_countryLongName] == thisCountry),
								flag_ecdc_timeLeftBeforeCrossingItaly
							] = daysTillCrossing
						
		
	return df_thisData


# ######################################################################################################################
def func_getUserName():
	username = getpass.getuser()

	print("COVID STATs Tool used by " + username)

	return username


# ######################################################################################################################
def func_replaceCountryNamesToHarmonizeWithAnnex20MasterList(
	thisDF,
	flagCountryColumn
):
	for thisCountry in dict_oneMillionDifferentCountryNames:
		print("thisCountry " + thisCountry + " will be replaced by " + dict_oneMillionDifferentCountryNames[thisCountry])
		thisDF.loc[
			(thisDF[flagCountryColumn] == thisCountry),
			flagCountryColumn
		] = dict_oneMillionDifferentCountryNames[thisCountry]

	return thisDF


# ######################################################################################################################
def func_doTheFinalNameConversion(
	thisDF,
	flagCountryColumn
):
	for thisColumn in thisDF.columns:
		print("column: " + thisColumn)

	for thisCountry in dict_finalNamesBeforeSavingData:
		print("thisCountry " + thisCountry + " will be replaced by " + dict_finalNamesBeforeSavingData[thisCountry])
		thisDF.loc[
			(thisDF[flagCountryColumn] == thisCountry),
			flagCountryColumn
		] = dict_finalNamesBeforeSavingData[thisCountry]

	return thisDF

# ######################################################################################################################
print("### CMG Covid-19 Statistics V" + str(_version_) + " made in Python by Thomas Rosenkranz @ CMG")
print(informationAboutLastVersion)
print(chr(10) + "Covid-19 data on country level based on JHU (Johns Hopkins University & Medicine)")
print("Covid-19 data on German-District level from RKI via ArcGIS NPGEO corona data hub")

username = func_getUserName()

flag_workInTogetherCMG = func_getPathToSaveFiles(username)

flag_saveCopyInCostaGroupSharedDrive = func_saveCopyInCostaGroupSharedDrive(username)

df_annex20CountryList = func_readAnnex20CountryList(flag_workInTogetherCMG)

if flag_doTheStatsUsing_JHU:
	df_unitedNations = func_readSourceData_UnitedNations(flag_workInTogetherCMG)
	df_eurostatPopulation = func_readSourceData_Eurostat(flag_workInTogetherCMG)
	df_worldBankPopulation = func_readSourceData_WorldBank(flag_workInTogetherCMG)
	
#region ECDC DATA DOWNLOAD & PREPARE
if flag_doTheStatsUsing_ECDC:
	flag_Datasource = flag_fileName_ecdc
	df_covidData_ECDC = func_readDataFromSourceOrFromHdIfAvailableAlready(flag_Datasource)

	# df_covidData_ECDC = func_replaceCountryNamesToHarmonizeWithAnnex20MasterList(df_covidData_ECDC)

	func_doAllAroundSavingThisSourceDataset(
		df_covidData_ECDC, flag_Datasource, "RAW",
		flag_workInTogetherCMG, flag_saveCopyInCostaGroupSharedDrive)
	
	df_covidData_ECDC = func_prepareThisCovidData(df_covidData_ECDC, flag_Datasource, flag_workInTogetherCMG)
	
	df_covidData_ECDC = func_compareIR_inRelationToItalyPerDay(
		df_covidData_ECDC,
		df_annex20CountryList,
		dict_ecdc_dataset["old_daily"]
	)
	
	df_covidData_ECDC = func_fill_IR_DeltaFigures(df_covidData_ECDC)
	
	df_covidData_ECDC = func_calculateIRPredictionForCrossingItaly(df_covidData_ECDC, df_annex20CountryList)
	
	func_exportFinalFileIntoPBIFolder(df_covidData_ECDC, flag_Datasource, flag_workInTogetherCMG)
	
	func_doAllAroundSavingThisSourceDataset(
		df_covidData_ECDC, flag_Datasource, "PREPARED",
		flag_workInTogetherCMG, flag_saveCopyInCostaGroupSharedDrive)
#endregion

# region ECDC DATA DOWNLOAD & PREPARE
if flag_doTheStatsUsing_ECDC_Weekly:
	flag_Datasource = flag_fileName_ecdcWeekly
	df_covidData_ECDC = func_readDataFromSourceOrFromHdIfAvailableAlready(flag_Datasource)

	# df_covidData_ECDC = func_replaceCountryNamesToHarmonizeWithAnnex20MasterList(df_covidData_ECDC)

	func_doAllAroundSavingThisSourceDataset(
		df_covidData_ECDC, flag_Datasource, "RAW",
		flag_workInTogetherCMG, flag_saveCopyInCostaGroupSharedDrive)
	
	df_covidData_ECDC = func_prepareThisCovidData(df_covidData_ECDC, flag_Datasource, flag_workInTogetherCMG)
	#
	df_covidData_ECDC = func_compareIR_inRelationToItalyPerDay(
		df_covidData_ECDC,
		df_annex20CountryList,
		dict_ecdc_dataset["new_weekly"]
	)

	# not needed
	# df_covidData_ECDC = func_fill_IR_DeltaFigures(df_covidData_ECDC)
	
	# df_covidData_ECDC = func_calculateIRPredictionForCrossingItaly(df_covidData_ECDC, df_annex20CountryList)
	#

	df_covidData_ECDC = func_doTheFinalNameConversion(df_covidData_ECDC, flag_ecdcWeekly_countriesAndTerritories)

	func_exportFinalFileIntoPBIFolder(df_covidData_ECDC, flag_Datasource, flag_workInTogetherCMG, )
	#
	func_doAllAroundSavingThisSourceDataset(
		df_covidData_ECDC, flag_Datasource, "PREPARED",
		flag_workInTogetherCMG, flag_saveCopyInCostaGroupSharedDrive)
# endregion

#region JHU DATA DOWNLOAD & PREPARE
if flag_doTheStatsUsing_JHU:
	flag_Datasource = flag_fileName_jhu
	df_covidData_JHU = func_readDataFromSourceOrFromHdIfAvailableAlready(flag_Datasource)
	
	func_doAllAroundSavingThisSourceDataset(
		df_covidData_JHU, flag_Datasource, "RAW",
		flag_workInTogetherCMG, flag_saveCopyInCostaGroupSharedDrive)

	df_covidData_JHU = func_prepareThisCovidData(df_covidData_JHU, flag_Datasource, flag_workInTogetherCMG)

	df_covidData_JHU = func_doTheFinalNameConversion(df_covidData_JHU, flag_jhu_converted_country)

	func_exportFinalFileIntoPBIFolder(df_covidData_JHU, flag_Datasource, flag_workInTogetherCMG)
	
	func_doAllAroundSavingThisSourceDataset(
		df_covidData_JHU, flag_Datasource, "PREPARED",
		flag_workInTogetherCMG, flag_saveCopyInCostaGroupSharedDrive)
#endregion

#region RKI DATA DOWNLOAD & PREPARE
if flag_doTheGermanDistricts_RKI_YESTERDAY:
	flag_Datasource = flag_fileName_RKI_District_Yesterday
	df_RKI_Districts_Yesterday = func_readDataFromRKI_via_NPGEO_InJson()
	
	func_doAllAroundSavingThisSourceDataset(
		df_RKI_Districts_Yesterday, flag_Datasource, "RAW",
		flag_workInTogetherCMG, flag_saveCopyInCostaGroupSharedDrive)
	
	func_exportFinalFileIntoPBIFolder(df_RKI_Districts_Yesterday, flag_Datasource, flag_workInTogetherCMG)
	func_doAllAroundSavingThisSourceDataset(
		df_RKI_Districts_Yesterday, flag_Datasource, "PREPARED",
		flag_workInTogetherCMG, flag_saveCopyInCostaGroupSharedDrive)
#endregion

print(chr(10) + "### that was fun, COVID stats are done, time to get a coffee ... pls press enter")
choice = input().lower()
if choice in yes:
	print(">> good bye")
elif choice in no:
	print(">> good bye")