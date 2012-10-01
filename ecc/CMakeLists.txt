SET(ECC_SRC ecc_recover.c lfsr.c discrete_math.c)
ADD_LIBRARY(ecc OBJECT ${ECC_SRC})
