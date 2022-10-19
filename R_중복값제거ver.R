 

nonfood_history_raw <- read_sheet(ss = "sheets address",

                                  sheet = "22년_raw")



 a <- nonfood_history_raw %>% arrange(desc(VOC인입일시))

 

 names(a) <- c("dt", "voc_cd", "voc_dt", "ord_dt", "rdate", "ddt", "pkg", "ord_cd", "cluster_center_code", "delivery", "md", "voc_catg_3", "contents", "sply_cd",

                "sply_nm", "prd_cd", "prd_nm", "sell_pos_term", "tem", "ord_type", "goods_zone", "cate_1_nm", "catg_2_nm")


 b <- a %>% distinct( ord_cd, prd_cd, .keep_all=T)


 c <- b[,c(1:23)] 

   

 names(c) <- c("공유일자", "VOC번호", "VOC인입일시", "주문일시", "수령일", "배송완료일시", "포장방법", "주문번호", "센터코드", "배송정책", "담당MD", "VOC카테고리(소)",

               "VOC내용", "공급사코드", "공급사명", "상품코드", "prd_cd", "판매가능일수", "보관온도", "보관유형", "적치존", "상품eSCM대카테고리", "상품eSCM중카테고리")

   
 d <- c
 

e <- d %>% arrange(VOC인입일시)


 write_sheet(ss = "sheets address",

             sheet = "22년_raw_3", data = d)

 