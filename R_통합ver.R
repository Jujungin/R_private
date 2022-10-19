
today <- today()

today_ch <- paste0("'", today, "'")

yesterday_ch <- case_when(

  weekdays(today) == "월요일" ~ paste0("'",today-3, "'"),

  TRUE ~ paste0("'",today-1, "'"))

yesterday <- date(substring(yesterday_ch,2,12))


day35 <- today-35

day35_ch <- paste0("'",day35, "'")

sales_day35 <- day35-1

sales_day35_ch <- paste0("'",sales_day35,"'")

sales_yesterday <- today-1

sales_yesterday_ch <- paste0("'",sales_yesterday,"'")



DB <- paste0("select ctc.voc_id , ctc.voc_dt , cog.payment_completed_at , coo.delivery_expected_date ,

              ctc.call_class_nm , ctc.call_cat_nm1 , ctc.call_cat_nm2 , ctc.call_cat_nm3 ,

              ctc.complain_type_nm , ctc.liability , ctc.action , ctc.order_no , ctc.goods_cd , 

              coo.business_type , sms.md , spi.deal_nm , ctc.voc_content , sms.sply_cd ,

              sms.sply_nm , coo.cluster_center_code , spi.catg_2_nm , spi.catg_3_nm,

              spi.catg_1_cd , spi.catg_2_cd , spi.catg_3_cd 

              from mkrs_schema.ckurly_tb_call as ctc

              left join commerce_order.orders as coo on coo.order_no = ctc.order_no

              left join commerce_order.group_order as cog on coo.group_order_no = cog.group_order_no 

              left join mkrs_aa_schema.u_md_sply_mthly_1d as sms on sms.master_cd = ctc.goods_cd 

              left join mkrs_aa_schema.u_prd_info_1d as spi on spi.master_cd = ctc.goods_cd 

              where (spi.catg_1_cd not in('FI_00000013', 'FI_00000014', 'FI_00000015', 'FI_00000016', 

              'FI_00000017', 'FI_00000018', 'FI_00000019', 'FI_00000020', 'FI_00000021', 'FI_00000022', 

              'FI_00000023', 'FI_00000024', 'FI_00000025', 'FI_00000026', 'FI_00000027', 'FI_00000028', 

              'FI_00000029', 'FI_00000030', 'FI_00000031', 'FI_00000032', 'FI_00000033', 'FI_00000034', 

              'FI_00000035') and spi.catg_2_cd not in('SE_00000051', 'SE_00000226', 'SE_00000227'))

              and ctc.voc_dt >= ", yesterday_ch )

 

DB1 <- dbGetQuery(redshift_conn, DB) %>% distinct(voc_id, .keep_all=T)


DB1$voc_dt <- as.POSIXct(DB1$voc_dt)

DB1$payment_completed_at <- as.Date(DB1$payment_completed_at)

DB1$delivery_expected_date <- as.Date(DB1$delivery_expected_date)

DB1$call_class_nm <- as.character(DB1$call_class_nm)

DB1$call_cat_nm1 <- as.character(DB1$call_cat_nm1)

DB1$call_cat_nm2 <- as.character(DB1$call_cat_nm2)

DB1$call_cat_nm3 <- as.character(DB1$call_cat_nm3)

DB1$complain_type_nm <- as.character(DB1$complain_type_nm)

DB1$liability <- as.character(DB1$liability)

DB1$action <- as.character(DB1$action)

DB1$order_no <- as.character(DB1$order_no)

DB1$goods_cd <- as.character(DB1$goods_cd)

DB1$business_type <- as.character(DB1$business_type)

DB1$md <- as.character(DB1$md)

DB1$deal_nm <- as.character(DB1$deal_nm)

DB1$voc_content <- as.character(DB1$voc_content)

DB1$sply_cd <- as.character(DB1$sply_cd)

DB1$sply_nm <- as.character(DB1$sply_nm)

DB1$cluster_center_code <- as.character(DB1$cluster_center_code)

DB1$catg_2_nm <- as.character(DB1$catg_2_nm)

DB1$catg_3_nm <- as.character(DB1$catg_3_nm)

DB1$order_no <- as.character(DB1$order_no)

DB1$voc_id <- as.character(DB1$voc_id)


max(DB1$voc_dt)

min(DB1$voc_dt)

 

 
DB2 <- arrange(DB1,desc(voc_dt), voc_id , payment_completed_at , delivery_expected_date , call_class_nm , 

               call_cat_nm1 , call_cat_nm2 , call_cat_nm3 , complain_type_nm , liability , action , order_no ,

               goods_cd , business_type , md , deal_nm , voc_content , sply_cd , sply_nm , cluster_center_code ,

               catg_2_nm , catg_3_nm  )

 

nonfood_words <- c()

risk_words <- c('상해', '폭발', '부상', '응급실', '화상', '화재', '누전', '감전', '스파크', '유해', '화학', '소견서', 'CriticalVOC', '크리티컬')

 

risk_n_words <- c('후기 선별', '후기선별', '후기 답변', '후기답변', '기처리', '속상해', '속상한', '속상했', '마다가스카', '이상해', '이상한', '이상했', '돈가스')


voc_today_keyword_real <- 

  DB2 %>% filter(call_cat_nm3 %in% c('냄새', '색', '크기/모양', '중량/수량', '상해/피부이상', '구성품 누락', '비식품 성능/작동/조립', '컬리품질보증제', 

                                  '상품 표기정보', '안전성/효능', '이용불만(항공권)', '그 외 항공권 문의', '이용불만(숙박상품)', '그 외 숙박상품 문의', 

                                  '이용불만(여행/문화)', '그 외 여행/문화 상품 문의', '썩음/곰팡이', '플라스틱/유리/비닐/고무 이물', '금속 이물', '원료 이물',

                                  '식품 내 벌레 이물', '개인위생이물(머리카락 등)', '기타 비금속 이물' )

                 ) %>% 



  mutate(risk_cnt = str_count(voc_content, paste(nonfood_words, collapse="|")),

         risk_n_cnt = str_count(voc_content, paste(risk_n_words, collapse="|")))%>%

  filter(!risk_n_cnt >0) %>% select(-risk_cnt, -risk_n_cnt)


voc_today_nonfood <- voc_today_keyword_real %>%  distinct(order_no, goods_cd, .keep_all = T) %>%

                     mutate(today = today()) %>% rename(dt=today)



voc_final_today <- voc_today_nonfood %>%

  select(-c(catg_1_cd, catg_2_cd)) %>% 

  select('공유일자'=dt, 'VOC번호'=voc_id, 'VOC인입일시'=voc_dt, '주문일시'=payment_completed_at, '배송완료일시'=delivery_expected_date,

         'VOC인입방법'=call_class_nm, 'VOC카테고리(대)'= call_cat_nm1, 'VOC카테고리(중)'=call_cat_nm2, 'VOC카테고리(소)'=call_cat_nm3,

         'VOC분류'=complain_type_nm, '귀책'=liability, '결과'=action, '주문번호'=order_no,'상품코드'=goods_cd, '상품타입'=business_type,

         '담당MD'=md, '상품명'=deal_nm, 'VOC내용'=voc_content, '공급사코드'=sply_cd, '공급사명'=sply_nm,

         '센터코드'=cluster_center_code, '상품eSCM대카테고리'=catg_2_nm,'상품eSCM중카테고리'= catg_3_nm)



voc_final_today$VOC인입일시 <- as.Date(voc_final_today$VOC인입일시)

voc_final_today$공유일자 <- as.Date(voc_final_today$공유일자)

voc_final_today$주문일시 <- as.Date(voc_final_today$주문일시)

voc_final_today$배송완료일시 <- as.Date(voc_final_today$배송완료일시)

 

voc_final_today$VOC번호 <- as.character(voc_final_today$VOC번호)

sheet_append(ss = "sheets address", sheet = "22년_raw_2", data = voc_final_today)


voc_final_today_risk <- voc_final_today %>% 

  mutate(risk_cnt = str_count(VOC내용, paste(risk_words, collapse="|")),

         risk_n_cnt = str_count(VOC내용, paste(risk_n_words, collapse="|"))) %>%

  filter(risk_cnt - risk_n_cnt >0) %>% select(-risk_cnt, -risk_n_cnt)

sheet_append(ss = "sheets address", sheet = "Risk_voc", data = voc_final_today_risk)
