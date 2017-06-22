delete from CMS_GROUPPRODN_SUMM_BK where GRPPRODNSUMMBKSEQ in (
select GRPPRODNSUMMBKSEQ from CMS_GROUPPRODN_SUMM_BK
where processdt='2017-02-28' and producercd in (
 select producercd from cms_producerchannel_m where CHANNELCD IN ( 'INHOUSE', 'BANKSTAFF')
)
)

delete from CMS_PROD_SUMMARY_DTL where PRODNSUMMDTLSEQ in (
select PRODNSUMMDTLSEQ from CMS_PROD_SUMMARY_DTL where PRODNSUMMSEQ in (
  select PRODNSUMMSEQ from cms_production_summary
where processdt='2017-02-28' and producercd in (
 select producercd from cms_producerchannel_m where CHANNELCD IN ( 'INHOUSE', 'BANKSTAFF')
)
)
)

delete from cms_production_summary where PRODNSUMMSEQ in (
select PRODNSUMMSEQ from cms_production_summary
where processdt='2017-02-28' and producercd in (
select producercd from cms_producerchannel_m where CHANNELCD IN ( 'INHOUSE', 'BANKSTAFF')
)
)