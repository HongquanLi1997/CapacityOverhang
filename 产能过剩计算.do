


*----------------------------------------------------------------
*                        数据初处理——数据转换
*----------------------------------------------------------------

*Step1 把Wind数据转化为dta
cd "/Users/lihongquan/Desktop/数据/Wind数据"
*--使用外部命令"readWind"解决
readWind ,var(GDZC) timeType(q) t0(2002q1) split splitN(2) erase
gen stkcd1 =substr(stkcd,1,6) 
drop stkcd 
rename stkcd1 stkcd
gen year=substr(time,1,4)
gen quarter=substr(time,6,6)
sort stkcd year quarter
save 固定资产原值.dta,replace

*Step2 把利润表数据转化为dta
cd "/Users/lihongquan/Desktop/数据/CSMAR数据"
clear
import delimited 利润表.csv,varnames(1) encoding(gb2312) 
save 利润表.dta,replace


*----------------------------------------------------------------
*                             利润表处理
*----------------------------------------------------------------

cd "/Users/lihongquan/Desktop/数据/CSMAR数据"
use 利润表.dta,clear

*提取年份-只保留所需样本2002-2018
gen year=real(substr(accper,1,4))  //从会计期间中提取年份
gen month=real(substr(accper,6,2))  //从会计期间中提取月份
drop if year==2000 |year==2001 |year==2019  //只保留所需样本2002-2019

*-----------[ 样本量362329 ]------------

*转换股票代码(使得wind和csmar统一，以便合并)
tostring stkcd,replace  // 将股票代码stkcd变成字符型
replace stkcd=substr("000000"+stkcd,-6,6)  // 补零

*更改所需变量名称及标签
label var year 年
label var month 月
label var stkcd 股票代码
rename b001101000 sales 
label var sales 市场需求（营业收入）
rename b001201000 cogs
label var cogs 生产成本（营业成本）
rename b001209000 salefee
label var salefee 生产成本（销售费用）
rename b001210000 manafee
label var manafee 生产成本（管理费用）
rename b002000000 netprof
label var netprof 是否有亏损（净利润）

*只保留合并报表"A"
keep if typrep=="A"   
*删除01-01期初数据,只要季度数据
drop if strmatch(accper, "*01-01*") //

*------------[ 样本量149060 ]------------

*定义季度
gen quarter=. 
replace quarter=1 if month==3
replace quarter=2 if month==6
replace quarter=3 if month==9
replace quarter=4 if month==12

*保留所需解释变量
keep stkcd year quarter accper sales cogs salefee manafee netprof 
order stkcd year quarter
save 解释变量.dta,replace

*补充行业代码(门类+大类)
import excel "/Users/lihongquan/Desktop/数据/CSMAR数据/A股行业代码.xlsx", sheet("全部A股") firstrow clear
rename 证券代码 stkcd
rename 所属证监会行业代码交易日期最新收盘日行业级别门类 industry
rename 所属证监会行业代码交易日期最新收盘日行业级别大类 industry1
keep stkcd industry industry1 //C为制造业
gen stkcd1 =substr(stkcd,1,6) 
drop stkcd
duplicates drop stkcd,force
rename stkcd1 stkcd
save 行业代码,replace

*合并行业代码
use 固定资产原值,clear
merge m:1 stkcd using 行业代码
keep if _merge==3
drop _merge
destring year quarter,replace
save 固定资产原值,replace

*----------------------------------------------------------------
*                             处理所需要的变量
*----------------------------------------------------------------

*在解释变量中合并被解释变量"固定资产原值"
use 解释变量,clear
merge 1:1 stkcd year quarter using 固定资产原值
keep if _merge==3   //沪市b股被剔除
drop _merge

*-----------[ 样本量139042 ]------------

keep if industry=="C"  //保留制造业样本

*------------[ 样本量79890 ]-------------

drop if strmatch(comp, "*ST*") //删除ST公司

*------------[ 样本量75237 ]-------------

*创建一个yq格式的连续时间变量便于时间序列处理
gen ymd = date(accper, "YMD")
format %td ymd
gen yq = qofd(ymd)
destring stkcd,replace
tsset stkcd yq

*求一、二、三、四季度的数据(原利润表数据为累加数据，除第一季度外均需要后一季度减去前一季度)
local vars "sales cogs salefee manafee netprof"
bys stkcd: gen gid = _n
foreach v of varlist `vars' {
replace `v'=0 if `v' == . 
bysort stkcd year : gen `v'1=`v'-`v'[_n-1]
replace `v'1=`v' if quarter==1
replace `v'1=`v' if gid==1
drop `v'
rename `v'1 `v'
}
drop gid

*使用二、四季度均值对固定资产原值第一季度和第三季度插补(固定资产原值只有年报和半年报有)
sort stkcd yq
bysort stkcd : replace GDZC=(L.GDZC+F.GDZC)*0.5 if GDZC==.

*使用均值法对固定资产原值连续缺失情况进行插补
gen GDZC1=GDZC
gen GDZC2=GDZC
bysort stkcd :replace GDZC1=GDZC1[_n-1] if GDZC1==.  //运行n次直至完全补齐
bysort stkcd :replace GDZC2=GDZC2[_n+1] if GDZC2==.  //运行n次直至完全补齐
gen GDZC3=(GDZC1+GDZC2)/2
replace GDZC=GDZC3 if GDZC==.
bysort stkcd :replace GDZC=GDZC[_n+1] if GDZC==.  //每个公司上市后的第一季度的数据使用第二季度来插补

* \\剩下的没有插补的均为该公司未披露过固定资产原值(20个观测值)，后续直接删除


*------------------------影响产能过剩的因素Z_it---------------------

drop if sales==.    //如果存在缺失值则无法计算 

*-----------[ 样本量75230 ]-------------

*计算远期营业收入下降百分比(q-4期以前的历史最高营业收入相比q-4期的下降百分比)
gen sales_q4=l4.sales 
replace sales_q4=0 if sales_q4==.
bysort stkcd: gen saless = sales_q4[1]  //取t期之前的最大值
bysort stkcd: replace saless = cond(sales_q4 > saless[_n-1], sales_q4, saless[_n-1]) if _n >= 2 //定义saless为t期之前的最大值
gen Distant_D=(saless-sales_q4)/saless  //q-4期相比q期的营业收入下降百分比，如果未下降，等于0
replace Distant_D=0 if Distant_D<0

*计算近期营业收入下降百分比(q-4期相比q期的营业收入下降百分比)
gen Recent_D=(l4.sales-sales)/l4.sales
replace Recent_D=0 if Recent_D<0
*计算过去四个季度是否存在亏损的虚拟变量
gen loss=0
forvalue i = 1/4{
bysort stkcd : replace loss=1 if l`i'.netprof<0
}


*------------------------影响理论产能的因素X_it---------------------

*取Sales / COGS / SGA q-3到q期之和的自然对数
gen sga=salefee+manafee
sort stkcd yq
local vars "sales cogs sga"
foreach v of varlist `vars' {
bysort stkcd : gen `v'1= log(L3.`v'+L2.`v'+L.`v'+`v')
}
save 中间数据.dta,replace

*无风险利率RF_rate
*-------------利率数据处理------------
*先生成一个无风险利率日数据便于计算beta
import delimited 无风险利率.csv, varnames(1) encoding(utf8) clear
keep clsdt nrrdata nrrdaydt
gen date=date(clsdt,"YMD")
format date %dCY-N-D 
gen RF_rate=(1+nrrdata/100)^(1/4)-1  //按照复利的方式计算季度的无风险利率
rename nrrdaydt RF_rate_day
keep RF_rate RF_rate_day date
save RF_rate_day.dta,replace 

*生成一个无风险利率季度数据进入SFA计算
import delimited 无风险利率.csv, varnames(1) encoding(utf8) clear
keep clsdt nrrdata nrrdaydt
gen date=date(clsdt,"YMD")
format date %dCY-N-D 
gen RF_rate=((1+nrrdata/100)^(1/4)-1)*100 //按照复利的方式计算季度的无风险利率
rename nrrdaydt RF_rate_day
gen year=real(substr(clsdt,1,4))
gen month=real(substr(clsdt,6,2))
gen day=real(substr(clsdt,9,2))
keep if month==3&day==31|month==6&day==30|month==9&day==30|month==12&day==31
gen quarter=.  //定义季度
replace quarter=1 if month==3
replace quarter=2 if month==6
replace quarter=3 if month==9
replace quarter=4 if month==12
keep RF_rate RF_rate_day year quarter date
save RF_rate.dta,replace

*把利率数据合并进解释变量中
use 中间数据.dta,clear
merge m:1 year quarter using RF_rate.dta
keep if _merge==3
drop _merge


*gen date=date(accper,"YMD")
*format date %dCY-N-D 
sort stkcd date
*系统风险Beta合并
*对数收益率的波动率Vol合并
merge m:1 stkcd year quarter using beta&vol.dta
drop if year==2002
drop if year==2019
keep if _merge==3
sort stkcd year quarter
drop if GDZC==.

rename sales1 Sales
rename sga1 SGA
rename cogs1 COGS
label var Sales q_3至q期营业收入之和的自然对数
label var SGA q_3至q期销售费用管理费用之和的自然对数
label var COGS q_3至q期营业成本之和的自然对数
label var Distant_D q_4期相比q期的营业收入下降百分比
label var Recent_D q_4期以前的历史最高营业收入相比q_4期的下降百分比
label var loss 过去四个季度是否存在亏损的虚拟变量
label var year 年份
label var quarter 季度
label var yq 时间
label var GDZC 固定资产
label var RF_rate 无风险利率
label var beta beta
label var vol 收益波动率

*极端值winsor处理
gen lnGDZC=log(GDZC*100000000)
winsor2 lnGDZC Sales SGA COGS beta beta1 vol, replace cuts(1 99)

keep stkcd year lnGDZC quarter yq Sales SGA COGS Distant_D Recent_D loss RF_rate industry1 vol beta beta1
sort stkcd year quarter

*去掉SFA模型中需要变量的缺失值
egen m=rowmiss(_all)
drop if m>0
gen industry=substr(industry1,2,2)

*处理行业代码
destring industry,replace
drop industry1

*----------------------------------------------------------------
*                             使用SFA计算
*----------------------------------------------------------------
*SFA计算
global 理论产能影响因素 "Sales COGS SGA RF_rate beta1 vol i.industry"
global 产能过剩影响因素 "Distant_D Recent_D loss"
*最后一个递归窗口的回归结果
sfcross lnGDZC $理论产能影响因素 ,noconstant nolog technique(bfgs) distribution(tnormal) cost emean($产能过剩影响因素,noconstant)
*Jondrow(1982)计算u的条件期望
predict u ,u 
gen co=1-(1/exp(u))  // 和Aretz and pope（2018）计算方法一致
sum co

*使用sfcross进行recursive estimate求出产能过剩u
gen u =.
forvalue i = 172/235{
sfcross lnGDZC $理论产能影响因素 if yq<=`i' , noconstant nolog technique(bfgs) distribution(tnormal) cost emean($产能过剩影响因素,noconstant)
predict u`i',u
replace u=u`i' if `i'==yq
}

gen co=1-(1/exp(u))  // 计算最终的产能过剩指标
