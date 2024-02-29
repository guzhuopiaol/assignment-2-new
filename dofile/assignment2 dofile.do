* Assignment 2 replication
* Since stata is hard to perform interpolation operations, I used python to fill in the missing quarterly data of S12 holding data. For the convenience of interpolation, I interpolated all years and quarters. Now I will pick the appropriate value.
* Clean the complete s12 data
use "/Users/yukerui/Desktop/complete s12 holding data.dta", clear
generate y = 1997 + floor(level_0/4)
generate m = 3 * (mod(level_0, 4) + 1)
sort fundno cusip y m
save  "/Users/yukerui/Desktop/complete s12 holding data.dta", replace

* Clean the existing s12 data
use "/Users/yukerui/Desktop/s12holding.dta", clear
gen y = year(fdate)
gen m = month(fdate)
sort fundno cusip y m
save "/Users/yukerui/Desktop/s12holding.dta", replace

* Merge the complete s12 data with the existing s12 data
use "/Users/yukerui/Desktop/complete s12 holding data.dta", clear
merge 1:1 fundno cusip y m using /Users/yukerui/Desktop/s12holding.dta 

* Remove values that do not meet the criteria
gen f = cond(missing(fdate), 1, 0)
by fundno cusip: gen start_new = f == 1 & f[_n-1] != 1
by fundno cusip: gen seq = sum(start_new)
sort fundno cusip seq
by fundno cusip seq: egen length = total(f)

* Here I assume that if a fund company does not disclose a stock for more than three consecutive quarters, I will regard the fund company as giving up holding the stock.
gen in_range = (y > 1997 | (y == 1997 & m >= 3)) & (y < 2004 | (y == 2004 & m <= 3))
drop if in_range & length > 3
gen date_flag = (y > 2004 | (y == 2004 & m >= 6)) & (y < 2007 | (y == 2007 & m <= 12))
drop if date_flag & _merge == 1

* Clean the merged data
drop _merge level_0 index f start_new seq length in_range date_flag

* Create an identifying variable to merge data with
gen key = cusip

* Generate a flag so that the key appears in the cusip in a different mutual fund once per quarter of the year and is marked sequentially. This way, the unique key y m flag identification variable combination can be obtained for merging.
sort key y m fundno
by key y m: gen flag = _n
sort key y m flag
save "/Users/yukerui/Desktop/complete s12 holding data", replace

* Clean CRSP data 
use "/Users/yukerui/Desktop/CRSP.dta", clear

* Select the data corresponding to the year and quarter
gen y = year(date)
gen m = month(date)
keep if m == 3 | m == 6 | m == 9 | m == 12 | m == 5

* Calculate market cap
* A negative price for PRC is a bid/ask average and not an actual closing price. Therefore I positive all PRCs.
replace PRC = abs(PRC)
gen price = PRC * CFACPR
gen market_cap = price * SHROUT * 1000 * CFACSHR
bysort y m PERMCO: egen cap = total(market_cap)
format market_cap cap %20.0g

* The market value of May 31 is generated here for subsequent regression
bysort y PERMCO: gen cap5 = cap if m == 5
bysort y PERMCO: egen capmay = max(cap5)
drop if m == 5
drop cap5
format capmay %20.0g

* The NCUSIP in the CRSP database corresponds to the CUSIP in S12, so I construct the corresponding identification variables for merging.
gen key = NCUSIP
gen flag = 1
drop if key == ""
sort key y m flag
save "/Users/yukerui/Desktop/CRSP.dta", replace

* Merge S12 holdings data with CRSP monthly data
use "/Users/yukerui/Desktop/complete s12 holding data.dta", clear
merge 1:1 key y m flag using /Users/yukerui/Desktop/CRSP.dta
drop if _merge == 2

* Fill in the data of each cusip in different mutual funds. The variables to be filled in are ERMNO date NAMEENDT NCUSIP PERMCO CUSIP PRC SHROUT CFACPR price market_cap cap.
sort key y m flag
by key y m: replace PERMNO = PERMNO[1] if flag != 1
by key y m: replace date = date[1] if flag != 1
by key y m: replace NAMEENDT = NAMEENDT[1] if flag != 1
by key y m: replace NCUSIP = NCUSIP[1] if flag != 1
by key y m: replace PERMCO = PERMCO[1] if flag != 1
by key y m: replace CUSIP = CUSIP[1] if flag != 1
by key y m: replace PRC = PRC[1] if flag != 1
by key y m: replace SHROUT = SHROUT[1] if flag != 1
by key y m: replace CFACPR = CFACPR[1] if flag != 1
by key y m: replace price = price[1] if flag != 1
by key y m: replace market_cap = market_cap[1] if flag != 1
by key y m: replace cap = cap[1] if flag != 1
by key y m: replace CFACSHR = CFACSHR[1] if flag != 1

* Determine whether the total value of the stocks held by the fund is greater than the company's market capitalization
gen cap1 = shares * price
bysort y m PERMCO: egen cap2 = total(cap1)
format cap1 cap2 %20.0g

* Delete values that do not meet authors' criteria
drop if cap2 > cap
drop _merge flag
drop if y == 2007 | y == 1997
save "/Users/yukerui/Desktop/complete s12 holding data.dta", replace
* The end of merging s12 holding data with crsp data

* Determine whether a mutual fund is active, passive, or unclassified
* Merge CRSP MFDB with MFLINK1
* Clean CRSP MFDB
use "/Users/yukerui/Desktop/CRSPMFDB.dta", clear
format crsp_fundno crsp_portno %10.0g
sort crsp_fundno crsp_portno 

* we can find a crsp_fundno corresponds to a fund_name. Therefore, I decided to delete crsp_portno that appeared multiple times to ensure that crsp_fundno and fund_name correspond one to one.
duplicates drop crsp_fundno, force
sort crsp_fundno

* Determine whether the fund name is active (ap=2) or passive (ap=1)
gen flag = 0
foreach pattern in "Index" "Idx" "Indx" "Ind " "Russell" "S & P" "S and P" "S&P" "SandP" "SP" "DOW" "Dow" "DJ" "MSCI" "Bloomberg" "KBW" "NASDAQ" "NYSE" "STOXX" "FTSE" "Wilshire" "Morningstar" "100" "400" "500" "600" "900" "1000" "1500" "2000" "5000" {
    replace flag = 1 if regexm(fund_name, "`pattern'")
}
gen ap = 0
replace ap = 1 if flag == 1 | index_fund_flag != ""
replace ap = 2 if ap == 0
drop flag index_fund_flag ncusip cusip8 crsp_portno
ren fund_name fundname

* Merge crsp mutual fund data with mflink1
merge m:1 crsp_fundno using "/Users/yukerui/Desktop/mflink1_raw.dta"
keep if _merge == 3
drop _merge crsp_fundno cusip8 fund_name nasdaq ncusip merge_fundno

* Through observation, the difference between wficn corresponding to different fundnames is that the share class is different. Some wficn will correspond to A, B, C and other types of funds. However, these name changes will not affect the active or passive nature of the fund. Therefore, I decided to duplicates drop wficn.
duplicates drop wficn, force

* Merge data with MFLINK2
merge 1:m wficn using "/Users/yukerui/Desktop/mflink2_raw.dta"
keep if _merge == 3
drop _merge country mgrcoab
gen y = year(fdate)
gen m = month(fdate)

* Merge data with s12 holdings
merge 1:m fundno y m using "/Users/yukerui/Desktop/complete s12 holding data.dta"
drop if _merge == 1

* If ap is missing, define the corresponding fund company as unclassified (ap=3)
replace ap = 3 if missing(ap)
drop _merge ioc num_holdings assets
save "/Users/yukerui/Desktop/s12.dta"
* The end of determining whether the fund name is active, passive or unclassified

* Select stocks in the Russell 1000 and 2000 indexes
* Clean Russell data
use "/Users/yukerui/Desktop/russell_all.dta", clear
gen y = .
tostring yearmonth, generate(strvar)

* Through observation, I discovered the correspondence between strvar and year. I use the following method for replacement.
replace y = 2006 if strvar == "557"
replace y = 2005 if strvar == "545"
replace y = 2004 if strvar == "533"
replace y = 2003 if strvar == "521"
replace y = 2002 if strvar == "509"
replace y = 2001 if strvar == "497"
replace y = 2000 if strvar == "485"
replace y = 1999 if strvar == "473"
replace y = 1998 if strvar == "461"

gen m = 6
ren cusip key
save "/Users/yukerui/Desktop/russell_all.dta", replace

* Merge data and delete stocks that are not within the established range
use "/Users/yukerui/Desktop/s12.dta", clear
merge m:1 key y m using "/Users/yukerui/Desktop/russell_all.dta"
save "/Users/yukerui/Desktop/s12.dta", replace

drop if m == 3 | m == 12
rename _merge me

* Get shares data for September
preserve 
keep if m == 9
keep fundno key PERMNO PERMCO CUSIP y shares SHROUT CFACSHR NCUSIP cusip
rename SHROUT SHROUT9
rename shares shares9
rename CFACSHR CFACSHR9
save "/Users/yukerui/Desktop/m9.dta", replace
restore

* Combine June and September data
keep if m == 6
merge 1:1 fundno key y using /Users/yukerui/Desktop/m9.dta
drop if _merge == 2

keep if me == 3
drop _merge me
format adj_mrktvalue %20.0g
save "/Users/yukerui/Desktop/s12.dta", replace

* Select the top 250 companies ranked by russell2000
keep if r2000 == 1
gen r = .
duplicates drop key y, force
gsort y -adj_mrktvalue
by y: gen top250 = _n
replace r = 1 if top250 <= 250 & top250 != .
keep if r == 1
keep key y r r2000
save "/Users/yukerui/Desktop/r1.dta"

* Select the bottom 250 companies ranked by russell1000
use "/Users/yukerui/Desktop/s12.dta", clear
keep if r2000 == 0
gen r = .
duplicates drop key y, force
gsort y adj_mrktvalue
by y: gen bottom250 = _n
replace r = 1 if bottom250 <= 250 & bottom250 != .
keep if r ==1
keep key y r r2000
rename r rr
save "/Users/yukerui/Desktop/r2.dta"

* Select firms in the 250 bandwidth around the cutoff
use "/Users/yukerui/Desktop/s12.dta", clear
merge m:1 key y using /Users/yukerui/Desktop/r1.dta
drop _merge
merge m:1 key y using /Users/yukerui/Desktop/r2.dta
drop _merge
keep if r == 1 | rr == 1
save "/Users/yukerui/Desktop/s12.dta", replace

* Calculate total mutual fund ownership, passive ownership, active ownership, unclassified ownership
format shares9 shares %20.0g

* Calculate Mutual fund ownership, passive, active and unclassified using September data.
preserve
gen total = shares9/(SHROUT9 * 1000 * CFACSHR9)

bysort PERMCO y: egen total_mf = sum(total)
bysort PERMCO y: egen passive = sum(total) if ap == 1
bysort PERMCO y: egen active = sum(total) if ap == 2
bysort PERMCO y: egen unclassified = sum(total) if ap == 3

bysort PERMCO y: egen total_mf_y = max(total_mf)
bysort PERMCO y: egen passive_y = max(passive) 
bysort PERMCO y: egen active_y = max(active) 
bysort PERMCO y: egen unclassified_y = max(unclassified )

replace total_mf_y = 0 if missing(total_mf_y)
replace passive_y = 0 if missing(passive_y)
replace active_y = 0 if missing(active_y)
replace unclassified_y = 0 if missing(unclassified_y)

sum total_mf_y passive_y active_y unclassified_y
restore

* Calculate Mutual fund ownership, passive, active and unclassified using June data.
gen total = shares/(SHROUT * 1000 * CFACSHR)
drop if missing(shares) | shares == 0

bysort PERMCO y: egen total_mf = sum(total)
bysort PERMCO y: egen passive = sum(total) if ap == 1
bysort PERMCO y: egen active = sum(total) if ap == 2
bysort PERMCO y: egen unclassified = sum(total) if ap == 3

bysort PERMCO y: egen total_mf_y = max(total_mf)
bysort PERMCO y: egen passive_y = max(passive) 
bysort PERMCO y: egen active_y = max(active) 
bysort PERMCO y: egen unclassified_y = max(unclassified )

replace total_mf_y = 0 if missing(total_mf_y)
replace passive_y = 0 if missing(passive_y)
replace active_y = 0 if missing(active_y)
replace unclassified_y = 0 if missing(unclassified_y)

sum total_mf_y passive_y active_y unclassified_y

* After comparison, I found that June is closer to the value in the paper. So I choose June data.
save "/Users/yukerui/Desktop/summary.dta", replace

* Calculate the percentage of independent directors on the boards of each firm for each year 
use "/Users/yukerui/Desktop/independent director.dta", clear
format YEAR %10.0g
generate is_I = (CLASSIFICATION == "I")
egen ID = mean(is_I), by(CUSIP YEAR)
duplicates drop CUSIP YEAR, force
rename YEAR y
rename CUSIP cusip6
save "/Users/yukerui/Desktop/independent director.dta", replace

* Merge data
use "/Users/yukerui/Desktop/summary.dta", clear
merge m:1 cusip6 y using "/Users/yukerui/Desktop/independent director.dta"
drop if _merge == 2
drop _merge
save "/Users/yukerui/Desktop/summary.dta", replace

* Calculate the poison pill removal and merge data
use "/Users/yukerui/Desktop/governance.dta", clear
format DUALCLASS PPILL LSPMT %10.0g
sort CN6 YEAR
by CN6: gen poison = PPILL == 0 & PPILL[_n-1] == 1
by CN6: gen greater = LSPMT == 0 & LSPMT[_n-1] == 1
rename YEAR y
rename CN6 cusip6
save "/Users/yukerui/Desktop/governance.dta", replace

use "/Users/yukerui/Desktop/summary.dta", clear
merge m:1 cusip6 y using "/Users/yukerui/Desktop/governance.dta"
drop if _merge == 2
drop _merge
save "/Users/yukerui/Desktop/summary.dta", replace

* Calculate the ROA and merge data
use "/Users/yukerui/Desktop/roa.dta"
drop gvkey indfmt consol popsrc datafmt curcd costat fdate
drop if missing(at) 
drop if missing(ni)
gen roa = ni / at 
gen y = year(datadate)
gen CUSIP = substr(cusip, 1, 8)
duplicates drop CUSIP y, force
drop cusip
save "/Users/yukerui/Desktop/roa.dta", replace

use "/Users/yukerui/Desktop/summary.dta", clear
merge m:1 CUSIP y using /Users/yukerui/Desktop/roa.dta
drop if _merge == 2
winsor2 roa, replace cut(1 99)
save "/Users/yukerui/Desktop/summary.dta", replace

* Do descriptive statistics
* Convert variable units to percentages
replace total_mf_y = total_mf_y * 100
replace passive_y = passive_y * 100
replace active_y = active_y * 100
replace unclassified_y = unclassified_y * 100
replace ID = ID * 100

duplicates drop key y, force

tabstat total_mf_y passive_y active_y unclassified_y ID poison greater DUALCLASS roa, s(mean p50 sd n) col(stat) f(%7.3f)

* Generate second table
* Fill in missing capmay values
bysort key y: egen maycap = max(capmay)

gen lnmarket = ln(cap)
gen lnmarket2 = lnmarket^2
gen lnmarket3 = lnmarket^3
gen lnfloat = ln(adj_mrktvalue)

reghdfe total_mf_y r2000 lnmarket lnmarket2 lnmarket3 lnfloat, absorb(y) vce(cluster CUSIP)
reghdfe passive_y r2000 lnmarket lnmarket2 lnmarket3 lnfloat, absorb(y) vce(cluster CUSIP)
reghdfe active_y r2000 lnmarket lnmarket2 lnmarket3 lnfloat, absorb(y) vce(cluster CUSIP)
reghdfe unclassified_y r2000 lnmarket lnmarket2 lnmarket3 lnfloat, absorb(y) vce(cluster CUSIP)

* Generate third table
egen passivesd = sd(passive_y)
gen passive1 = passive_y / passivesd
reghdfe passive1 r2000 lnmarket lnfloat, absorb(y) vce(cluster CUSIP)
reghdfe passive1 r2000 lnmarket lnmarket2 lnfloat, absorb(y) vce(cluster CUSIP)
reghdfe passive1 r2000 lnmarket lnmarket2 lnmarket3 lnfloat, absorb(y) vce(cluster CUSIP)

* Generate forth table
egen IDsd = sd(ID)
gen ID1 = ID / IDsd
ivreghdfe ID1 lnmarket lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)
ivreghdfe ID1 lnmarket lnmarket2 lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)
ivreghdfe ID1 lnmarket lnmarket2 lnmarket3 lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)

* Generate fifth table
preserve 
drop if y > 2002
ivreghdfe ID1 lnmarket lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)
ivreghdfe ID1 lnmarket lnmarket2 lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)
ivreghdfe ID1 lnmarket lnmarket2 lnmarket3 lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)
restore

preserve
drop if y < 2003
ivreghdfe ID1 lnmarket lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)
ivreghdfe ID1 lnmarket lnmarket2 lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)
ivreghdfe ID1 lnmarket lnmarket2 lnmarket3 lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)
restore

* Generate sixth table
egen greatersd = sd(greater)
gen greater1 =  greater / greatersd
ivreghdfe greater1 lnmarket lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)
ivreghdfe greater1 lnmarket lnmarket2 lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)
ivreghdfe greater1 lnmarket lnmarket2 lnmarket3 lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)


* Generate seventh table
egen DUALCLASSsd = sd(DUALCLASS)
gen DUALCLASS1 =  DUALCLASS / DUALCLASSsd
ivreghdfe DUALCLASS1 lnmarket lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)
ivreghdfe DUALCLASS1 lnmarket lnmarket2 lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)
ivreghdfe DUALCLASS1 lnmarket lnmarket2 lnmarket3 lnfloat (passive1 = r2000), absorb(y) cluster(CUSIP)
