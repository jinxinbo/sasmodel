option compress=yes validvarname=any;

libname dt 'E:\运营商欺诈模型\葫芦爬虫统计明细数据';
x 'cd E:\运营商欺诈模型\葫芦爬虫统计明细数据';

data dt.target;
   set dt.target1 dt.target2;
run;
proc sort data=dt.target nodupkey;by application_no;run;


/*以xx.data0713作为主键*/

proc sql;
   create table dt.data2_0713 as
    select a.*,b.*,d.*,d1.*,e.*,f.*,g.*,h.*,h1.*
	from xx.data0713 a 
	left join dt.target b on compress(a.app_no)=compress(b.application_no)
/*    left join dt.data0712  c on compress(a.app_no)=compress(c.app_no) --缺失较多，暂时不用*/
    left join xb.data_rule d on compress(a.app_no)=compress(d.app_no)
	left join xb.Data_message d1 on compress(a.app_no)=compress(d1.app_no)
    left join xx.data0712_other e on compress(a.app_no)=compress(e.m_app_no)
	left join xx.DATA_phone_count f on compress(a.app_no)=compress(f.app_no)
	left join xx.Xx_gh g on compress(a.app_no)=compress(g.app_no)
	left join xx.Calltime_zj h on compress(a.app_no)=compress(h.app_no)
	left join xx.Xx_strategy h1 on compress(a.app_no)=compress(h1.app_no)
   ;
quit;

proc sort data=dt.data2_0713 out=dt.data3_0713 nodupkey;by app_no;run;

data dt.data3_0713;
   set dt.data3_0713;
   where app_no not in('K2018010916002164549','K2018011117002166163');
run;

data dt.data1;
   set dt.data1;
/*     这些是通话详单解析字段问题*/
	 if out_use_cnt_3m=. and in_use_cnt_3m=. then delete;
/*  这些是联系人清洗错误*/
if contact1_intop_x=. or contact2_intop_x=. or  is_contact_intop20=. then delete;
run;

;

/*这个客户解析错误*/
N2017121109002145440
;


data data_train;
  set dt.Data3_0713;
  	where agr_fpd20=1 and app_no^='N2017121109002145440' and index(app_no,'K')=0;
   target=dd1>20;
     drop application_no status_adj  reg_time business_id_t3 business_id_t2 business_id_t1 cpd_bz cpd dd1 status	agr_fpd10 agr_fpd20 agr_fpd30 def_fpd30 m_app_no request_time id_no create_time;
  rename app_no=user_sid ;
/*  if out_use_cnt_3m=. and in_use_cnt_3m=. then delete;*/
;run;

data data_train;
   set data_train;
/*   format app_date1 yymmdd10.;*/
   app_mon=substr(compress(user_sid,,'kd'),1,8);
/*   app_date1=mdy(substr(app_date,5,2),substr(app_date,7,2),substr(app_date,1,4));*/
/*   app_month=substr(app_date,1,6);*/
/*   app_week=week(app_date1);*/
run;

/*不选择按月份抽样*/
proc sort data=data_train;by target ;run;
proc surveyselect data=data_train out=temp1(drop=SelectionProb SamplingWeight) noprint method=srs seed=100
samprate=0.8 outall;
strata target ;
run;

data dt.data_train dt.data_test;
  set temp1;
   if Selected=1 then output dt.data_train;
     else output dt.data_test;
   drop Selected;
run;


proc sql;
    create table err1 as
	 select app_mon,count(*) as cnt,avg(target) as badrate
	 from data_train
	 group by 1
	 ;
 quit;



/* data dt.data_train dt.data_valid;*/
/*     set data_train;*/
/*	 if substr(compress(user_sid,,'kd'),1,6)>'201708' then output dt.data_train;*/
/*	    else output dt.data_valid;*/
/*run;*/



/*data err;*/
/*   set dt.Data3_0713;*/
/*   if night_times_5=. then fg=1;else fg=2;*/
/*   keep app_no fg;*/
/*run;*/
/**/
/*data err1;*/
/*   set err;*/
/*   app_date=substr(compress(app_no,,'kd'),1,8);*/
/*run;*/
/**/
/*proc sql;*/
/*   create table err2 as */
/*   select app_date,fg,count(*) as cnt*/
/*   from err1*/
/*   group by 1,2*/
/*   ;*/
/*quit;*/
/*proc transpose data=err2 out=err3 prefix=p_;by app_date;id fg;run;*/





proc freq data=dt.data_train;table target;run;
proc freq data=dt.data_test;;table target;run;


%inc 'E:\sas代码\检查空缺情况_macro.sas';
%Inc 'E:\sas代码\c_5_2_统计属性分布_macro.sas';
%Inc 'E:\sas代码\c_5_1_统计连续型分布_macro.sas';

 %count_nullobs(inDS=dt.data_train,outDS=dt.nullobs2);
proc sort data=dt.nullobs2;
	by descending null_percent;
run;quit;

%table_univariate(inDS=dt.data_train,outDS=dt.univ2);
data del_univ;
   set dt.univ2;
   if pvalue_0=pvalue_96;
   put myvar;
 run;

/* 缺失值填充处理*/
if smses_cnt_1m=. Then smses_cnt_1m=0;
if smses_cnt_2m=. Then smses_cnt_2m=0;
if smses_cnt_3m=. Then smses_cnt_3m=0;
if smses_cnt_4m=. Then smses_cnt_4m=0;
if smses_cnt_5m=. Then smses_cnt_5m=0;
if smses_cnt_6m=. Then smses_cnt_6m=0;
if count_same_percent=. Then count_same_percent=0;
if total_time_3m=. Then total_time_3m=0;
if month_bz_3m=. Then month_bz_3m=0;
if total_counts_3m=. Then total_counts_3m=0;
if out_call_counts_3m=. Then out_call_counts_3m=0;
if in_call_counts_3m=. Then in_call_counts_3m=0;
if out_call_time_3m=. Then out_call_time_3m=0;
if in_call_time_3m=. Then in_call_time_3m=0;
if smses_cnt_trend36=-1 then smses_cnt_trend36=.;
if smses_cnt_trend24=-1 then smses_cnt_trend24=.;
if count_same_percent=. then count_same_percent=0;
if plan_amt<0 then plan_amt=0;
if is_contact_incalls<=0 then do;
	is_contact_incalls=.;
	is_contact_intop20=.;
	lxr_use_time_rate=.;
	call_out_cnt=.;
end;
if contact1_intop_x<=0 then contact1_intop_x=.;
if contact2_intop_x<=0 then contact2_intop_x=.;




filename dp catalog 'work.t1.dp.source';
data _null_;
   set dt.nullobs2 end=last;
   where null_percent>=0.85;
   file dp;
   if _n_=1 then put 'drop ';
  put varname;
  if last then put ';';
run;






/*data dt.data_train4;*/
/*    set dt.data_train;*/
/*   %inc dp;*/
/*run;   */

ods results off;
ods listing;

%macro nullobs;
data err2;
	length var $32. pvalue_0 pvalue_25 pvalue_50 pvalue_75 pvalue_90 pvalue_95 pvalue_100 8.;
	stop;
run;

data _null_;
  set dt.nullobs2 end=last;
   where null_percent<0.05 and nullnum>0 and vartype='num';
   call symputx('invar'||left(_n_),varname);
   if last then call symputx('n',_n_);
run;
%do i=1 %to &n.;
%let invar=&&invar&i..;
	data err;
	set dt.data_train;
	where &invar.^=.;
	keep &inVAR. user_sid;
	run;

	proc univariate data=err noprint;
	var &inVAR.;
	output out=err1 pctlpts=0 25  50 75 90 95 100
					pctlpre=pvalue
					pctlname=_0 _25  _50 _75 _90 _95 _100;
	run;quit;

	data err1;
	set err1;
	var="&invar.";	
	run;
proc append base=err2 data=err1 force;run;quit;
%end;

%mend;

%nullobs;



filename null catalog 'work.t1.null.source';
data _null_;
  set err2;
  where var not in('smses_cnt_trend36',
  'smses_cnt_trend24',
'contact1_intop_x',
'contact2_intop_x',
'is_contact_incalls',
'is_contact_intop20',
'lxr_use_time_rate',
'call_out_cnt'
);
  file null;
  put 'if ' var '=. then ' var '=' pvalue_50';';
run;

data fk;
   set dt.data_train;
   where is_contact_incalls>0 and contact1_intop_x<=0;
   keep user_sid is_contact_incalls contact1_intop_x contact2_intop_x;
run;



data dt.data_train3;
  set dt.data_train;
if smses_cnt_1m=. Then smses_cnt_1m=0;
if smses_cnt_2m=. Then smses_cnt_2m=0;
if smses_cnt_3m=. Then smses_cnt_3m=0;
if smses_cnt_4m=. Then smses_cnt_4m=0;
if smses_cnt_5m=. Then smses_cnt_5m=0;
if smses_cnt_6m=. Then smses_cnt_6m=0;
if count_same_percent=. Then count_same_percent=0;
if total_time_3m=. Then total_time_3m=0;
if month_bz_3m=. Then month_bz_3m=0;
if total_counts_3m=. Then total_counts_3m=0;
if out_call_counts_3m=. Then out_call_counts_3m=0;
if in_call_counts_3m=. Then in_call_counts_3m=0;
if out_call_time_3m=. Then out_call_time_3m=0;
if in_call_time_3m=. Then in_call_time_3m=0;
if smses_cnt_trend36=-1 then smses_cnt_trend36=.;
if smses_cnt_trend24=-1 then smses_cnt_trend24=.;
if count_same_percent=. then count_same_percent=0;
if plan_amt<0 then plan_amt=0;
if is_contact_incalls<=0 then do;
	is_contact_incalls=.;
	is_contact_intop20=.;
	lxr_use_time_rate=.;
	call_out_cnt=.;
end;
if contact1_intop_x<=0 then contact1_intop_x=.;
if contact2_intop_x<=0 then contact2_intop_x=.;
  %inc dp;
%inc null;
run;

data dt.data_train4;
     set dt.data_train3;
 run;



proc contents data=dt.data_train4 out=cnt(keep=name type) noprint;run;
proc sort data=cnt;by name;run;
/*data cnt;*/
/*   set cnt;*/
/*   len=length(name);*/
/*   if len>=30;*/
/*run;*/

filename ren catalog 'work.t1.ren.source';
data cnt;
    set cnt end=last;
	file ren;
	where type=1 and name^='target';
	varname='name'||left(_n_);
	if _n_=1 then put 'rename ';
	put name '=' varname ;
	if last then put ';';
 run;


 /*--类别变量分箱--*/

proc contents data=dt.data_train4 out=cnt(keep=name type) noprint;run;
filename fl catalog 'work.t1.fl.source';
data cnt2;
   set cnt end=last;
   where type=2;
   file fl;
   if _n_=1 then put 'keep user_sid target';
   put name;
   if last then put ';';
run;

data test;
  set dt.data_train4;
   %inc fl;
;run;



%inc 'E:\pos贷行为评分卡\macro_分组方法.sas';
%calcwoe_catevar(inDS=test,outDS=test_1);

/*%Bestgrouping(test_1,target,7,dt.new_fl_mapiv,dt.new_fl_woetab,0.05);*/

%calWoeIV_and_apply(inDS=test_1,adj_num=0.1,
                           min_grpNum=0.1,
                           outDS=dt.new_fl_woetab,
                           outMapSum=dt.new_fl_map,outIVSum=dt.new_fl_iv,max_group=8,my_woe=1);


proc sql;
  create table dt.new_fl_mapiv as
   select a.*,b.ll,b.ul
   from dt.new_fl_iv a
   left join dt.new_fl_map b
   on a.varname=b.varname and a.grp_var=b.bin;
quit;

proc sort data=dt.new_fl_mapiv;by varname grp_var;run;
data iv1;
   set dt.new_fl_mapiv;by varname grp_var;
   if last.varname and iv>=0.018;
   put varname;
run;

data newwoe;
   set dt.new_fl_woetab;
   keep user_sid target w_:;
 run;

proc sql;
  create table test2 as
  select a.*,b.*
  from test as a
  left join newwoe as b on a.user_sid=b.user_sid;
quit;


;

%macro xx();
data _null_;
    set iv1 end=last;
	call symputx('name'||left(_n_),varname);
	if last then call symputx('n',_n_);
run;
%do i=1 %to &n.;
%let varname=&&name&i.;
proc sort data=test2 out=dt.wds_&varname.(keep=&varname. w_&varname.) nodupkey;by &varname.;run;
%end;
%mend;
%xx();



filename lx catalog 'work.t1.lx.source';
data cnt2;
   set cnt end=last;
   where type=1;
   file lx;
   if _n_=1 then put 'keep user_sid target';
   put name;
   if last then put ';';
run;

data dt.data_train4_lx;
  set dt.data_train4;
  %inc lx;
/*  %inc ren;*/
;run;



%inc 'E:\pos贷行为评分卡\macro_分组方法.sas';
%inc 'E:\pos贷行为评分卡\macro_检查分箱的稳定性.sas';


/*-----------------*/
%macro select_var_bywoe;
%do wi=0 %to 0;
 %do bi=4 %to 8;
proc printto log=".\变量分箱_&wi..txt" new;run;quit;
%calWoeIV_and_apply(inDS=dt.data_train4_lx,adj_num=0.1,
                           min_grpNum=0.05,
                           outDS=dt.new_lx_woetab_&wi._&bi.,
                           outMapSum=dt.new_lx_map_&wi._&bi.,outIVSum=dt.new_lx_iv_&wi._&bi.,max_group=&bi.,my_woe=&wi.);

proc printto;run;quit;

proc sql;
  create table dt.new_lx_mapiv_&wi._&bi. as
   select a.*,b.ll,b.ul
   from dt.new_lx_iv_&wi._&bi. a
   left join dt.new_lx_map_&wi._&bi. b
   on a.varname=b.varname and a.grp_var=b.bin;
quit;
%end;
%end;

%mend;

%select_var_bywoe;


proc sort data=dt.new_lx_mapiv_1_8;by varname grp_var;run;
data iv3;
   set dt.new_lx_mapiv_1_8;
   by varname grp_var;
   if last.varname and iv>=0.02;
   put varname;
run;

/*计算woe值的跳点*/

%macro cal_woe_jump;
%do ci=4 %to 8;
data mp;
   set dt.New_lx_mapiv_0_&ci.;
   where GRP_VAR^=0;
run;
proc sort data=mp;by varname GRP_VAR;run;
data mp1;
   set mp;
   by varname GRP_VAR;
   retain woe_fg 0;
   if first.varname then woe_fg=woe;
    else do; 
	   if woe_fg>woe then fg=-1;
	     else fg=1;
	   woe_fg=woe;
	  end;
 run;

 proc sql;
    create table mp2 as
	 select varname,max(iv) as iv,max(case when fg=1 then GRP_VAR else 0 end) as a_rank,
	           sum(case when fg=1 then fg else 0 end) as a_sum,
              max(case when fg=-1 then GRP_VAR else 0 end) as d_rank,
			  sum(case when fg=-1 then -fg else 0 end) as d_sum,
			  max(GRP_VAR) as GRP_VAR
	   from mp1
	   group by 1;
  quit;

data mp3;
   set mp2;
   num=&ci.;
   where iv>=0.015;
   if (a_sum=1 and (a_rank=2 or a_rank=GRP_VAR)) or (d_sum=1 and(d_rank=2 or d_rank=GRP_VAR))  or a_sum=0 or d_sum=0;
   keep varname iv GRP_VAR;
   rename iv=iv_&ci. GRP_VAR=GRP_VAR_&ci.;
run;

	%if &ci=4 %then %do;
	   data out_woe;
	      set mp3;
		run;
	 %end;

	 %else %do;
	 proc sql;
	    create table out_woe as 
		  select a.*,b.*
		  from out_woe a
		  left join mp3 b on a.varname=b.varname;
	  quit;
	%end;

%end;
%mend;

%cal_woe_jump;

proc sql;
   create table out_woe1 as
    select a.*,b.iv as iv0,b.GRP_VAR as GRP_VAR0
	from out_woe a
	left join iv3 b on a.varname=b.varname;
quit;

data out_woe2;
   set out_woe1;
   array ar{*} iv_:;
   do i=1 to dim(ar);
	  if ar(i)>temp then do;temp=ar(i);s_name=vname(ar(i));end;
	end;
  keep varname iv: s_name;
run;

data out_woe3;
   set out_woe2;
   if iv0=. then iv0=0.019;
   iv_s=max(of iv:);
   div=sum(iv_s,-iv0);
   keep varname s_name iv_s iv0 div;
run;
proc sort data=out_woe3;by descending div;run;


data _null_;
   set Out_woe3;
   where div>=0.01 and  s_name='iv_4' and iv_s>=0.02;
   put varname;
run;

/*5组*/
phone_incalls1m_pre
phone_incalls_6m
phone_incalls3m_totalpre
phone_incalls6m_pre
call_outcnt_trend1234
phone_incalls5m_pre
phone_incalls3m_pre
phone_incalls_3m
call_cnt_trend1234



in_use_cnt_3m
phone_outcalls_1m
call_out_time_34
call_incnt_trend1234
phone_outcalls1m_totalpre
mob_month2_var4
phone_incalls4m_totalpre
phone_incalls4m_pre
phone_incalls_4m
phone_outcalls1m_pre
out_call_time_3m
outcalls_man_3m
call_in_cnt_34
mob_month1_var6_1
mob_month2_var7_1
call_in_time_34
mob_month1_var7
incalls_man_4m
mob_month3_var4
phone_outcalls_4m
call_out_cnt_34
call_cnt
mob_month1_var7_1
avg_gh_t20_use_time
incalls_man_5m
r_gh_t20_call_in_time
smses_cnt_4m
phone_incalls_2m
mob_month2_var10_1
gh_use_t20_in_time
mob_month3_var12
rate_time
phone_incalls_1m

;
data mapiv0;
      set  dt.New_lx_mapiv_0_5(where=(varname in('phone_incalls1m_pre',
'phone_incalls_6m',
'phone_incalls3m_totalpre',
'phone_incalls6m_pre',
'call_outcnt_trend1234',
'phone_incalls5m_pre',
'phone_incalls3m_pre',
'phone_incalls_3m',
'call_cnt_trend1234'
)))
dt.New_lx_mapiv_0_4(where=(varname in('in_use_cnt_3m',
'phone_outcalls_1m',
'call_out_time_34',
'call_incnt_trend1234',
'phone_outcalls1m_totalpre',
'mob_month2_var4',
'phone_incalls4m_totalpre',
'phone_incalls4m_pre',
'phone_incalls_4m',
'phone_outcalls1m_pre',
'out_call_time_3m',
'outcalls_man_3m',
'call_in_cnt_34',
'mob_month1_var6_1',
'mob_month2_var7_1',
'call_in_time_34',
'mob_month1_var7',
'incalls_man_4m',
'mob_month3_var4',
'phone_outcalls_4m',
'call_out_cnt_34',
'call_cnt',
'mob_month1_var7_1',
'avg_gh_t20_use_time',
'incalls_man_5m',
'r_gh_t20_call_in_time',
'smses_cnt_4m',
'phone_incalls_2m',
'mob_month2_var10_1',
'gh_use_t20_in_time',
'mob_month3_var12',
'rate_time',
'phone_incalls_1m'
)));
run;







/*------------------------------------分界线--------------------------------------------*/





/*葫芦月账单缺失有点多*/
proc contents data=dt.Data0712 noprint out=cnt(keep=name type);run;
proc contents data=xx.Gh_stat noprint out=cnt1(keep=name type);run;

proc sql;
   create table iv31 as
    select * from iv3
	where varname not in(select name from cnt where type=1) and varname not in(select name from cnt1 where type=1)
	order by iv desc;
quit;



proc sql;
   create table mapiv as
    select * from dt.New_lx_mapiv_1_8
	where varname in(select varname from iv31);
 quit;




/*模型验证*/

 filename xx catalog 'work.t1.t3.source';
data _null_;	
   set mapiv;
   by varname GRP_VAR;
   file xx;
   if first.varname then put 'if 'varname'<='ul 'then W_'varname'=' woe';';
     else if last.varname=0 then  put 'else if 'varname'<='ul 'then W_'varname'=' woe';';
	   else put 'else W_'varname'=' woe';';
run;



data dt.data_test;
   set dt.Data3_0713;
/*	where def_fpd30='';*/
run;

data test_code;
   input app_no $32.;
datalines;
K2018050321002223746
K2018050411002223909
K2018050811002225076
K2018051011002226016
K2018051013002226066
K2018051217002227084
K2018051712002229707
K2018051715002229755
K2018051915002230643
K2018052113002231407
K2018052414002232566
;run;

proc sql;
   create table dt.data_test as
   select * from dt.Data3_0712
   where app_no in(select app_no from test_code);
quit;




data valid1;
   set dt.Data_test;
     if count_same_percent=. then count_same_percent=0;
%inc null;
run;




 %macro vd;
 data _null_;
     set iv1 end=last;
	 call symputx('var'||left(_n_),varname);
	 if last then call symputx('n',_n_);
 run;
 %do i=1 %to &n.;
 %let varname=&&var&i..;
 %put &varname.;
proc sql;
    create table valid1 as
	select a.*,b.w_&varname.
	from valid1 as a
	left join dt.Wds_&varname. as b on a.&varname.=b.&varname.;
quit;
%end;
%mend;

%vd;

data valid1;
   set valid1;
   %inc xx;
run;

data valid1_1;
   set valid1;
   keep dd1 app_no W_: status;
run;


proc transpose data=valid1_1 out=valid1_2;run;



data inmod1;
   set iv31;
  _name_='W_'||left(varname);
run;


/*模型变量分布稳定性分析*/
%macro wdx(intab,outtab);
data &outtab.;
    length var $32. count percent woe 8.;
    stop;
 run;
data _null_;
    set inmod1 end=last;
	where _name_ not in('Intercept','_LNLIKE_');
	call symputx('var'||left(_n_),_name_);
	if last then call symputx('n',_n_);
 run;
%do i=1 %to &n.;
       %put &&var&i..;
		%let varname=&&var&i..;
		proc freq data=&intab. noprint ;table &varname./out=cc;run;
		data cc;
		    set cc;
			rename &varname.=woe;
			var="&varname.";
		run;
      proc append base=&outtab. data=cc force;run;quit;
%end;
%mend;

%wdx(dt.New_lx_woetab_1_8,wdx_train);

%wdx(valid1_1,wdx_test1);
%wdx(valid1_1(where=(index(app_no,'K'))),wdx_test2);

%wdx(valid1_1(where=(status='A')),wdx_valid1);
%wdx(valid1_1(where=(status='A' and index(app_no,'K'))),wdx_valid2);

proc sql;
    create table wdx_all as
    select a.*,b1.count as count1,b1.percent/100 as percent1,
                b2.count as count2,b2.percent/100 as percent2,
				b3.count as count3,b3.percent/100 as percent3,
				b4.count as count4,b4.percent/100 as percent4,
               c.ll,c.ul,c.grp_var,c.cnt_0,c.cnt_1,c.iv
	from wdx_train a
	left join wdx_test1 b1 on a.var=b1.var and round(a.woe,1e-6)=round(b1.woe,1e-6)
	left join wdx_valid1 b2 on a.var=b2.var and round(a.woe,1e-6)=round(b2.woe,1e-6)
	left join wdx_test2 b3 on a.var=b3.var and round(a.woe,1e-6)=round(b3.woe,1e-6)
	left join wdx_valid2 b4 on a.var=b4.var and round(a.woe,1e-6)=round(b4.woe,1e-6)
	left join dt.New_lx_mapiv_1_8 c on a.var=cats('W_',c.varname) and round(a.woe,1e-6)=round(c.woe,1e-6);

quit;





%macro var_bin(intab,outtab);
data &outtab.;
    length var $32. p0 p1 woe 8.;
    stop;
 run;
data _null_;
    set inmod1 end=last;
	where _name_ not in('Intercept','_LNLIKE_');
	call symputx('var'||left(_n_),_name_);
	if last then call symputx('n',_n_);
 run;
%do i=1 %to &n.;
       %put &&var&i..;
		%let varname=&&var&i..;
		proc freq data=&intab. noprint ;table &varname.*target/out=cc;run;
		proc transpose data=cc out=cc1(drop=_name_ _label_) prefix=p;
		 by &varname.;
		 id target;
		 var count;
		data cc1;
		    set cc1;
			rename &varname.=woe;
			var="&varname.";
		run;
      proc append base=&outtab. data=cc1 force;run;quit;
%end;
%mend;


%var_bin(dt.New_lx_woetab_1_8(where=(index(user_sid,'K'))),wdx_train1);


proc sql;
    create table wdx_all1 as
    select a.*,c.ll,c.ul,c.grp_var,c.cnt_0,c.cnt_1,c.iv
	from wdx_train1 a
left join dt.New_lx_mapiv_1_8 c on a.var=cats('W_',c.varname) and round(a.woe,1e-6)=round(c.woe,1e-6);
quit;


proc sql;
   create table wdx_all2 as
    select a.*,b.*
	from wdx_all a
	left join wdx_all1 b on a.var=b.var and a.grp_var=b.grp_var
	;
quit;




1.call_incnt_trend1234 >=2.7 and in_use_cnt_1m [7.42]
2.call_incnt_trend1234 >=2.7 and in_use_cnt_1m [0.6]
3.call_incnt_trend1234 >=2.7 and in_use_cnt_1m <=42
;




data temp;
    set valid1;
    if call_incnt_trend1234 >=2.7 and 7<=in_use_cnt_1m<=42 then fg1=1;else fg1=0;
    if call_incnt_trend1234 >=2.7 and 0<=in_use_cnt_1m<=6 then fg2=1;else fg2=0;
    if call_incnt_trend1234 >=2.7 and in_use_cnt_1m<=42 then fg3=1;else fg3=0;
	if index(app_no,'K') then kn=1;else kn=0;
 run;

/**/
/* proc sql;*/
/*    select distinct agr_fpd10*/
/*	from temp;*/
/* quit;*/


proc sql;
   create table temp1 as
    select kn,count(*) as t1,
	     sum(case when status='A' then 1 else 0 end) as a1,
		 sum(case when agr_fpd20=1 and dd1>20 then 1 else 0 end) as f_b1,
		 sum(case when agr_fpd20=1 and dd1<=20 then 1 else 0 end) as f_g1,
		 sum(case when agr_fpd20=1 and cpd>20 then 1 else 0 end) as c_b1,
		 sum(case when agr_fpd20=1 and cpd<=20 then 1 else 0 end) as c_g1,
		 sum(case when agr_fpd10=1 and cpd>10 then 1 else 0 end) as t_b1,
		 sum(case when agr_fpd10=1 and cpd<=10 then 1 else 0 end) as t_g1
	  from temp
     where fg1=1
	 group by 1
	 ;
	  create table temp2 as
    select kn,count(*) as t1,
	     sum(case when status='A' then 1 else 0 end) as a1,
		 sum(case when agr_fpd20=1 and dd1>20 then 1 else 0 end) as f_b1,
		 sum(case when agr_fpd20=1 and dd1<=20 then 1 else 0 end) as f_g1,
		 sum(case when agr_fpd20=1 and cpd>20 then 1 else 0 end) as c_b1,
		 sum(case when agr_fpd20=1 and cpd<=20 then 1 else 0 end) as c_g1,
		 sum(case when agr_fpd10=1 and cpd>10 then 1 else 0 end) as t_b1,
		 sum(case when agr_fpd10=1 and cpd<=10 then 1 else 0 end) as t_g1
	  from temp
     where fg2=1
	 group by 1
	 ;
	    create table temp3 as
    select kn,count(*) as t1,
	     sum(case when status='A' then 1 else 0 end) as a1,
		 sum(case when agr_fpd20=1 and dd1>20 then 1 else 0 end) as f_b1,
		 sum(case when agr_fpd20=1 and dd1<=20 then 1 else 0 end) as f_g1,
		 sum(case when agr_fpd20=1 and cpd>20 then 1 else 0 end) as c_b1,
		 sum(case when agr_fpd20=1 and cpd<=20 then 1 else 0 end) as c_g1,
		 sum(case when agr_fpd10=1 and cpd>10 then 1 else 0 end) as t_b1,
		 sum(case when agr_fpd10=1 and cpd<=10 then 1 else 0 end) as t_g1
	  from temp
     where fg3=1
	 group by 1
	 ;
 quit;

data temp4;
   set temp1 temp2 temp3;
run;

/* proc freq data=temp;table fg1;run;*/
/* proc freq data=temp(where=(status='A'));table fg1;run;*/
/* proc freq data=temp(where=(agr_fpd20^=.));table fg1*target;run;*/
