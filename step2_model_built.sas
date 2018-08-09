%inc "E:\python代码及论文\合影\sas代码\c_6_4_CalcKS_macro.sas";
ods listing;
ods results off;

proc sort data=dt.New_lx_mapiv_1_8;by varname grp_var;run;
data iv3;
   set dt.New_lx_mapiv_1_8;
   by varname grp_var;
   if last.varname and iv>=0.02;
   put varname;
run;

proc sql;
    create table mapiv1 as 
	  select * from dt.new_lx_mapiv_1_8
	  where varname in(select varname from iv3) 
          and varname not in(select varname from mapiv0);
quit;

/*新增变量0724*/
proc sort data=d1.New_lx_mapiv_1_8;by varname grp_var;run;
data iv4;
   set d1.New_lx_mapiv_1_8;
   by varname grp_var;
   if last.varname and iv>=0.02;
   put varname;
run;
proc sql;
    create table mapiv2 as 
	  select * from d1.new_lx_mapiv_1_8
	  where varname in(select varname from iv4) ;
quit;

/*新增变量0726*/
proc sort data=d2.New_lx_mapiv_1_8;by varname grp_var;run;
data iv5;
   set d2.New_lx_mapiv_1_8;
   by varname grp_var;
   if last.varname and iv>=0.02;
   put varname;
run;
proc sql;
    create table mapiv3 as 
	  select * from d2.new_lx_mapiv_1_8
	  where varname in(select varname from iv5) ;
quit;

/* 最终分组*/
 filename xx catalog 'work.t1.t3.source';
data mapiv;
    set mapiv0 mapiv1 mapiv2 mapiv3;
	rename GRP_VAR=bin;
proc sort data=mapiv;by varname bin;run;

data _null_;
   set mapiv;
   by varname bin;
   file xx;
   if first.varname then put 'if 'varname'<='ul 'then W_'varname'=' woe';';
     else if last.varname=0 then  put 'else if 'varname'<='ul 'then W_'varname'=' woe';';
	   else put 'else W_'varname'=' woe';';
run;



proc sql;
    create table data_train4_lx as 
	select a.*,b.*,c.*
	from dt.data_train4_lx a
	left join d1.Data_0724_train b on a.user_sid=b.user_sid
	left join d2.data_train c on a.user_sid=c.user_sid
   ;
quit;

data allwoetab;
    set data_train4_lx;
	%inc xx;
	keep target user_sid w_:;
run;


data alltab1;
   set allwoetab;
run;

%macro cal_iv(tab,outtab);
data &outtab.;
   length name $32. iv 8.;
   stop;
run;

proc contents data=&tab. out=cnt(keep=name) noprint;run;quit;
data _null_;
   set cnt end=last;
   where index(name,'W_');
   call symputx('name'||left(_n_),name);
   if last then call symputx('n',_n_);
 run;

 %do wi=1 %to &n.;
%let name=&&name&wi..;
proc sql noprint; 
   select sum(target) , count(*)-sum(target) into : p1 , :p0 from &tab.;
quit;
proc freq data=&tab. noprint;table &name.*target/out=cc;
proc transpose data=cc out=cc1(drop=_name_ _label_) prefix=p;
 by &name.;
 id target;
 var count;

data cc1;
   set cc1 end=last;
   retain iv;
   maptotal=sum(p0,p1);
   p1t=p1/&p1.;
   p0t=p0/&p0.;
   p1_p0=p1/p0;
   woe=log(p1t/p0t);
   iv1=(p1t-p0t)*woe;
   iv+iv1;
   if last;
   name="&name.";
   keep name iv;
run;

proc append base=&outtab. data=cc1 force;run;quit;

%end;
%mend;

%cal_iv(alltab1,outiv);
proc sort data=outiv;by descending iv;run;


data _null_;
   set outiv;
   where iv<0.03;
   put name;
run;



/*iv值较小删除*/

/*data alltab1;*/
/*   set alltab1;*/
/*   drop W_call_cnt*/
/*W_incalls_man_3m*/
/*W_smses_cnt_trend36*/
/*W_mob_month3_var13*/
/*W_in_use_cnt_1m*/
/*W_rate_time*/
/*W_incalls_sum_1m*/
/*W_stb_usetime_var10*/
/*W_mob_month1_var1*/
/*W_in_use_cnt_3m*/
/*W_rate_cnt*/
/*W_phone_incalls_2m*/
/*W_mob_month1_var6_1*/
/*W_mob_month1_var3*/
/*W_incalls_sum_3m*/
/*W_in_use_time_1m*/
/*W_smses_cnt_6m*/
/*W_in_use_time_3m*/
/*W_total_counts_3m*/
/*W_stb_usecnt_var12*/
/*W_mob_month1_var7_1*/
/*W_call_in_time_34*/
/*W_in_call_counts_3m*/
/*W_out_call_counts_3m*/
/*W_smses_cnt_1m*/
/*W_outcalls_man_5m*/
/*W_outcalls_man_4m*/
/*W_call_intime_trend1234*/
/*W_mob_month1_var2*/
/*W_mob_month3_var1*/
/*W_mob_month3_var10_1*/
/*W_out_use_cnt_3m*/
/*W_in_use_cnt_2m*/
/*W_stb_usecnt_var3*/
/*W_mob_month3_var3*/
/*W_mob_month3_var2*/
/*W_incalls_sum_5m*/
/*W_mob_month2_var2*/
/*W_avg_gh_t20_use_in_time*/
/*W_mob_month1_var15_1*/
/*W_stb_usecnt_var0*/
/*W_incalls_sum_4m*/
/*W_call_in_cnt_12*/
/*W_incalls_sum_2m*/
/*W_incalls_man_1m*/
/*W_call_out_time_34*/
/*W_stb_usecnt_var11*/
/*W_mob_month1_var11_1*/
/*W_mob_month3_var5*/
/*W_mob_month3_var14_1*/
/*W_outcalls_sum_3m*/
/*W_call_out_cnt_34*/
/*W_stb_usetime_var3*/
/*W_stb_usecnt_var2*/
/*W_mob_month2_var5*/
/*W_call_in_cnt_34*/
/*W_mob_month1_var13*/
/*W_mob_month2_var15_1*/
/*W_phone_outcalls2m_pre*/
/*W_mob_month1_var14_1*/
/*W_stb_usecnt_var9*/
/*W_mob_month1_var10_1*/
/*W_stb_usetime_var9*/
/*W_mob_month2_var13*/
/*W_in_use_time_2m*/
/*W_out_use_time_3m*/
/*W_stb_usetime_var12*/
/*W_mob_month2_var10_1*/
/*W_tot_34*/
/*W_mob_month2_var1*/
/*W_outcalls_man_3m*/
/*W_call_out_cnt_12*/
/*W_outcalls_sum_2m*/
/*W_out_use_time_1m*/
/*W_stb_usetime_var11*/
/*W_phone_outcalls3m_pre*/
/*W_tot_12*/
/*W_phone_outcalls5m_pre*/
/*;run;*/



/*变量聚类*/
%inc "E:\python代码及论文\合影\sas代码\c_6_1_变量聚类_macro.sas";

%varclus(alltab1,0.7,outstat,outtree,outcluster,outr2);

proc sql;
    create table outcluster1 as
	 select a.*,b.iv,d.RSquareRatio
	 from outcluster a
	 left join outiv b on a.varname=b.name
	 left join outr2 d on a.varname=d.Variable
    order by cluster_1,iv desc;
 quit;


 data outcluster1;
    set outcluster1;
	by cluster_1 descending iv;
	if first.cluster_1 then grp=0;
	grp+1;
run;

proc sort data=outcluster1;by cluster_1 RSquareRatio;run;
data outcluster1;
   set outcluster1;
   by cluster_1 RSquareRatio;
	if first.cluster_1 then r2bin=0;
	r2bin+1;
run;

data outcluster2;
   set outcluster1;
   where grp<=2 and r2bin<=2;
   put varname;
run;


/*W_call_cnt_trend1234待修改*/
proc freq data=alltab1;table target;run;


ods listing;
ods results off;



data alltab2;
    set alltab1;
	if target=1 then weight=1;
	   else weight=1;
	   /*	drop W_First_confirm_time W_reg_confirm_time W_gjj_confirm_time;*/
	keep user_sid target weight
W_mob_month1_var8
W_phone_incalls6m_totalpre
W_stb_usetime_var0
W_mob_month3_var7
W_usetime_deeptime_var1
W_phone_incalls_6m
W_mob_month1_var9
W_mob_month1_var11_1
W_mob_month2_var10
W_smses_cnt_3m
W_mob_month1_var5
W_incalls_sum_3m
W_stb_usetime_var4
W_outcalls_man_5m
W_phone_outcalls2m_pre
W_mob_month2_var7_1
W_call_incnt_trend1234
W_phone_incalls5m_pre
W_call_outtime_trend1234
W_lxr_use_time_rate
W_total_amount_34
W_phone_outcalls1m_totalpre
W_call_out_time_34
W_mob_month2_var4
W_avg_gh_t20_use_in_time
W_mob_month1_var1
W_r_gh_t20_call_in_time
W_outcalls_sum_2m
W_call_in_cnt_34
W_smses_cnt_4m
W_stb_usecnt_var9
W_mob_month1_var2
W_phone_outcalls_1m
W_mob_month2_var10_1
W_call_out_cnt_34
W_rate_cnt
W_rate_time
W_mob_month2_var15_1
W_out_call_time_3m
W_mob_month1_var12
W_usecnt_deeptime_var1
W_in_use_cnt_3m
W_smses_cnt_trend24
W_mob_month1_var7
W_phone_outcalls_4m
W_incalls_man_5m
W_avg_gh_t20_use_time
W_mob_month3_var10_1
W_call_cnt
W_stb_usecnt_var12
W_smses_cnt_1m
W_phone_incalls_2m
W_total_amount_12
W_out_use_cnt_2m
W_mob_month3_var13
W_call_out_cnt
W_mob_month2_var6_1
W_outcalls_man_3m
W_incalls_man_1m
W_mob_month3_var2
W_phone_incalls2m_pre
W_total_amount_trend1234
W_gh_use_t20_in_time
;
;run;

/*data alltab2;*/
/*   set alltab2;*/
/*   drop W_disk_size W_disk_rate W_disk_free_size;*/
/*run;*/
 	


%inc 'E:\pos贷行为评分卡\macro_模型变量选择.sas';

proc printto log=".\model_train_log.txt" new;run;
%select_var(alltab2,70,LLINEAL_CONTACT_TYPE1,0.07);
proc printto;run;

proc sort data=pe1;by descending WaldChiSq;run;

proc sql;
   create table pp as
    select a.*,b.iv
	from pe1 a
	left join iv3 b on substr(a.name,3)=b.varname
    order by b.iv desc;
 quit;

proc printto;run;
proc transpose data=dt.test_model_tr out=inmod(drop=_label_);run;quit;
data inmod1;
   set inmod;
   where target^=.;
   put _NAME_;
run;

filename vd catalog 'work.vd.vd.source';
data yz;
   set inmod1 end=last;
   file vd;
   where _name_^='_LNLIKE_';
   if _n_=1 then put 'tmp=exp(sum(';
   if _name_='Intercept' then put target;
    else do;
   line1=catx('*',target,_name_);
   line=cats(',',line1);
   put line;
   end;
  if last then put '));';
run;


%KSStat(dt.test_model_logi, fraud_p, target, DSKS, M_KS);
proc sql noprint;
 select max(KS) into :M_KS from DSKS;
run; quit;
%put m_ks=&m_ks;

ods listing;
proc sort data=dt.test_model_logi;by descending fraud_p;run;
proc rank data=dt.test_model_logi groups=10 descending out=rank; 
var fraud_p; 
ranks rk; 
run; 
proc freq data=rank;table rk*target/nopercent nocol;run;



/*模型验证*/

 filename xx catalog 'work.t1.t3.source';
data _null_;	
   set mapiv;
   by varname bin;
   file xx;
   if first.varname then put 'if 'varname'<='ul 'then W_'varname'=' woe';';
     else if last.varname=0 then  put 'else if 'varname'<='ul 'then W_'varname'=' woe';';
	   else put 'else W_'varname'=' woe';';
run;


proc sql;
    create table Data_test as
	 select a.*,b.*,c.*
	 from dt.Data_test a
	 left join d1.Data_0724 b on a.user_sid=b.user_sid
     left join d2.Data3_old_add_cycle c on a.user_sid=c.user_sid ;
quit;


data valid1;
   set Data_test;
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
/*新增变量*/
if total_amount_12=. then total_amount_12=0;
if total_amount_34=. then total_amount_34=0;
if is_contact_incalls<=0 then do;
	is_contact_incalls=.;
	is_contact_intop20=.;
	lxr_use_time_rate=.;
	call_out_cnt=.;
end;
if contact1_intop_x<=0 then contact1_intop_x=.;
if contact2_intop_x<=0 then contact2_intop_x=.;
%inc null;
run;


/* %macro vd;*/
/* data _null_;*/
/*     set iv1 end=last;*/
/*	 call symputx('var'||left(_n_),varname);*/
/*	 if last then call symputx('n',_n_);*/
/* run;*/
/* %do i=1 %to &n.;*/
/* %let varname=&&var&i..;*/
/* %put &varname.;*/
/*proc sql;*/
/*    create table valid1 as*/
/*	select a.*,b.w_&varname.*/
/*	from valid1 as a*/
/*	left join dt.Wds_&varname. as b on a.&varname.=b.&varname.;*/
/*quit;*/
/*%end;*/
/*%mend;*/
/**/
/*%vd;*/

data valid1;
   set valid1;
   %inc xx;
run;


/*模型验证*/
data valid1_1;
	set valid1;
	%Inc vd;
	fraud_p=tmp/(1+tmp);
run;

proc sort data=valid1_1;by descending fraud_p;run;

%KSStat(valid1_1, fraud_p, target, DSKS, M_KS);
proc sql noprint;
 select max(KS) into :V_KS from DSKS;
run; quit;
%put v_ks=&v_ks;




/*卡牛数据验证*/

data err;
  set dt.Data3_0713;
  where agr_fpd20=1 and app_no^='N2017121109002145440' ;
  if index(app_no,'K') then channel=1;
    else channel=2;
  if dd1>10 then def_fpd10=1;else def_fpd10=0;
  if dd1>20 then def_fpd20=1;else def_fpd20=0;
  if dd1>30 then def_fpd30=1;else def_fpd30=0;
  if cpd>30 then cpd_fg=1;else cpd_fg=0;
run;

proc freq data=err(where=(channel=1));table def_fpd20*def_fpd30;run;




proc sql;
    create table Data_valid as
	 select a.*,b.*,c.*
	 from dt.Data3_0713 a
	 left join xx.data_stb b on a.app_no=b.app_no
     left join d2.Data3_old_add c on a.app_no=c.app_no;
quit;

data valid2;
  set Data_valid;
  	where agr_fpd20=1 and app_no^='N2017121109002145440' and index(app_no,'K');
    target=cpd>20;
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
/*新增变量*/
if total_amount_12=. then total_amount_12=0;
if total_amount_34=. then total_amount_34=0;
if is_contact_incalls<=0 then do;
	is_contact_incalls=.;
	is_contact_intop20=.;
	lxr_use_time_rate=.;
	call_out_cnt=.;
end;
if contact1_intop_x<=0 then contact1_intop_x=.;
if contact2_intop_x<=0 then contact2_intop_x=.;
%inc null;
run;

proc freq data=valid2;table target;run;


data valid2;
   set valid2;
   %inc xx;
run;


data valid2_1;
	set valid2;
	%Inc vd;
	fraud_p=tmp/(1+tmp);
run;

proc sort data=valid2_1;by descending fraud_p;run;

%KSStat(valid2_1, fraud_p, target, DSKS, M_KS);
proc sql noprint;
 select max(KS) into :V_KS from DSKS;
run; quit;
%put v_ks=&v_ks;



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

%wdx(dt.test_model_logi,wdx_train);

%wdx(valid1_1,wdx_test);

%wdx(valid2_1,wdx_valid);

proc sql;
    create table wdx_all as
    select a.*,b1.count as count_test,b1.percent as percent_test,b2.count as count_valid,b2.percent as percent_valid,c.target as coef
	from wdx_train a
	left join wdx_test b1 on a.var=b1.var and round(a.woe,1e-6)=round(b1.woe,1e-6)
	left join wdx_valid b2 on a.var=b2.var and round(a.woe,1e-6)=round(b2.woe,1e-6)
	left join inmod1 c on a.var=c._NAME_;
quit;

data wdx_all;
   set wdx_all;
   psi1=log(percent_test/percent)*(percent_test-percent)/100;
   psi2=log(percent_valid/percent)*(percent_valid-percent)/100;
run;
proc sql;
   create table wdx_all1 as
    select var,abs(sum(psi1)) as psi_test,abs(sum(psi2)) as psi_valid
	from wdx_all
	group by 1
	order by psi_valid desc;
quit;



 /*随机生成模型*/

%inc 'E:\pos贷行为评分卡\macro_随机挑选模型.sas';

/*权重设置为1*/

data alltab2;
    set alltab1;
	if target=1 then weight=1;
	   else weight=1;
	keep user_sid target weight
/*W_avg_gh_t20_use_time*/
/*W_call_incnt_trend1234*/
/*W_gh_use_t20_in_time*/
/*W_in_use_cnt_3m*/
/*W_incalls_man_1m*/
/*W_incalls_man_5m*/
/*W_lxr_use_time_rate*/
/*W_mob_month1_var5*/
/*W_mob_month1_var11_1*/
/*W_mob_month2_var4*/
/*W_mob_month2_var10_1*/
/*W_mob_month2_var15_1*/
/*W_mob_month3_var13*/
/*W_mob_month3_var10_1*/
/*W_out_call_time_3m*/
/*W_phone_incalls5m_pre*/
/*W_phone_incalls_2m*/
/*W_phone_incalls_6m*/
/*W_phone_outcalls_1m*/
/*W_phone_outcalls_4m*/
/*W_smses_cnt_4m*/
/*W_stb_usecnt_var12*/
/*W_stb_usetime_var4*/
/*W_total_amount_trend1234*/
/*W_usecnt_deeptime_var1*/
/*W_usetime_deeptime_var1*/
W_usecnt_deeptime_var1
W_total_amount_trend1234
W_mob_month3_var13
W_phone_incalls5m_pre
W_mob_month1_var11_1
W_phone_incalls_6m
W_out_call_time_3m
W_gh_use_t20_in_time
W_stb_usecnt_var12
;run;


/*剔除变量*/
%put &num.;
data _null_;
   set dt.rod2_var&num.;
   if _n_=10;
   put v1 v2;
run;


data alltab2;
   set alltab2;
   drop W_in_use_cnt_3m W_mob_month2_var15_1
   W_mob_month1_var5 W_stb_usetime_var4
   W_mob_month2_var10_1 W_smses_cnt_4m
W_call_incnt_trend1234 W_mob_month3_var10_1
W_phone_outcalls_4m W_usetime_deeptime_var1
W_incalls_man_1m W_phone_outcalls_1m
W_incalls_man_5m W_lxr_use_time_rate
W_avg_gh_t20_use_time W_mob_month2_var4

;run;

/*--------------------------------------*/

/*剔除变量后开始*/
proc contents data=alltab2 out=var(keep=name) noprint;run;
data var;
  set var end=last;
  where name not in('user_sid','target','weight');
  if last then call symputx('num',_n_);
  rename name=varname;
run;

%put &num.;
%let num=&num.;


proc transpose data=var out=var1(drop=_name_ _label_) prefix=v;var varname;run;quit;


data dt.rod1_var&num.;
    set var1;
   array x[&num.] v1-v&num.;
   n=dim(x);
   k=1;
   ncomb=comb(n,k);
   do j=1 to ncomb;
      call allcomb(j, k, of x[*]);
	  output;
   end;
run;


data dt.rod2_var&num.;
    set var1;
   array x[&num.] v1-v&num.;
   n=dim(x);
   k=2;
   ncomb=comb(n,k);
   do j=1 to ncomb;
      call allcomb(j, k, of x[*]);
	  output;
   end;
run;

%put &num.;

x 'cd E:\运营商欺诈模型\葫芦爬虫统计明细数据';


proc printto log=".\model_train_log.txt"  new;run;		

%put start at %sysfunc(time(),time.);
ods results off;
%combine(intab=alltab2,res=dt.re_resk2_var&num.,out=dt.re_outk2_var&num.,ns1=dt.rod2_var&num.,ns2=catx('',v1,v2),group=10,sp_var=ttttt);
ods results on;
%put end at %sysfunc(time(),time.);
proc printto;run;quit;



/*%let num=15;*/
proc sort data=dt.re_outk2_var&num. out=outmod3;by num descending v1 rk;run;
data outmod3_1;
   set outmod3;
   by num descending v1 rk;
   if first.num then rk_v=0;
    else rk_v+1;
run;

proc sort data=outmod3_1;by num descending m1 rk;run;
data outmod3_1;
   set outmod3_1;
   by num descending m1 rk;
   if first.num then rk_m=0;
    else rk_m+1;
run;
proc sql;
   create table xx as
    select num,sum(case when rk=rk_v then 1 else 0 end) as count1,sum(case when rk=rk_m then 1 else 0 end) as count2
	from outmod3_1
	group by 1
	;
quit;
proc sql;
   create table xx1 as
    select a.*,b.*,ks1-ks2 as div
	from xx as a
	left join dt.Re_resk2_var&num. as b
	on a.num=b.num 
	having  count2>=8 
   order by ks2 desc   ;
 quit;




 proc sql;
    create table out_mapiv1 as
	 select * from dt.new_lx_mapiv_1
	 where varname in(select  compress(lowcase(tranwrd(upcase(_NAME_),'W_',''))) from inmod1);
quit;
proc sql;
    create table out_mapiv2 as
	 select * from dt.New_fl_mapiv
	 where varname in(select  compress(lowcase(tranwrd(upcase(_NAME_),'W_',''))) from inmod1);
quit;
data out_mapiv;
    set out_mapiv1 out_mapiv2;
 run;



%let tab=rebintab;
%let name=W_dif_apply_day;
%let outcc=dif_apply_day;
proc sql noprint; 
   select sum(target) , count(*)-sum(target) into : p1 , :p0 from &tab.;
quit;
proc freq data=&tab. noprint;table &name.*target/out=cc;
proc transpose data=cc out=cc1(drop=_name_ _label_) prefix=p;
 by &name.;
 id target;
 var count;
data &outcc.;
   set cc1;
   retain iv;
   maptotal=sum(p0,p1);
   p1t=p1/&p1.;
   p0t=p0/&p0.;
   p1_p0=p1/p0;
   woe=log(p1t/p0t);
   iv1=(p1t-p0t)*woe;
   iv+iv1;
   put woe;
   rename &name.=bin;
   varname="&name.";
run;


data inmod;
input name $32.;
datalines;
W_age
W_al_m6_cell_notbank_allnum
W_al_m6_id_notbank_allnum
W_app_city_contacts
W_disk_rate
W_disk_size
W_dura_auth_page
W_dura_bankcard_num
W_dura_dep_type
W_dura_emp_add
W_emp_city_contacts
W_name_avgcnt
W_num_dup_cnt
W_rate_emp_city_contacts
W_rate_ex_short_cnt
W_rate_name_dup_cnt
W_rate_name_if_dup
W_rate_name_maxcnt
W_rate_name_null_cnt
W_rate_num_charcnt
W_rate_num_dup_cnt
W_rate_spchar_cnt
W_rate_without_areacode_cnt
W_request_our
W_td_finaldecision
;
run;


data inmod;
   set inmod;
   varname=compress(tranwrd(_NAME_,'W_',''));
run;
proc sql;
   create table mmm as
    select * from mapiv
	where varname in(select varname from inmod);
 quit;
