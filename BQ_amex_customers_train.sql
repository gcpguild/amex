CREATE EXTERNAL TABLE `hci-project-313508.amex_credit_card.amex_customers`
(
		customer_ID STRING,
		S_2 DATE,
		P_2 FLOAT64,
		D_39 FLOAT64,
		B_1 FLOAT64,
		B_2 FLOAT64,
		R_1 FLOAT64,
		S_3 FLOAT64,
		D_41 FLOAT64,
		B_3 FLOAT64,
		D_42 FLOAT64,
		D_43 FLOAT64,
		D_44 FLOAT64,
		B_4 FLOAT64,
		D_45 FLOAT64,
		B_5 FLOAT64,
		R_2 FLOAT64,
		D_46 FLOAT64,
		D_47 FLOAT64,
		D_48 FLOAT64,
		D_49 FLOAT64,
		B_6 FLOAT64,
		B_7 FLOAT64,
		B_8 FLOAT64,
		D_50 FLOAT64,
		D_51 FLOAT64,
		B_9 FLOAT64,
		R_3 FLOAT64,
		D_52 FLOAT64,
		P_3 FLOAT64,
		B_10 FLOAT64,
		D_53 FLOAT64,
		S_5 FLOAT64,
		B_11 FLOAT64,
		S_6 FLOAT64,
		D_54 FLOAT64,
		R_4 FLOAT64,
		S_7 FLOAT64,
		B_12 FLOAT64,
		S_8 FLOAT64,
		D_55 FLOAT64,
		D_56 FLOAT64,
		B_13 FLOAT64,
		R_5 FLOAT64,
		D_58 FLOAT64,
		S_9 FLOAT64,
		B_14 FLOAT64,
		D_59 FLOAT64,
		D_60 FLOAT64,
		D_61 FLOAT64,
		B_15 FLOAT64,
		S_11 FLOAT64,
		D_62 FLOAT64,
		D_63 STRING,
		D_64 STRING,
		D_65 FLOAT64,
		B_16 FLOAT64,
		B_17 FLOAT64,
		B_18 FLOAT64,
		B_19 FLOAT64,
		D_66 FLOAT64,
		B_20 FLOAT64,
		D_68 FLOAT64,
		S_12 FLOAT64,
		R_6 FLOAT64,
		S_13 FLOAT64,
		B_21 FLOAT64,
		D_69 FLOAT64,
		B_22 FLOAT64,
		D_70 FLOAT64,
		D_71 FLOAT64,
		D_72 FLOAT64,
		S_15 FLOAT64,
		B_23 FLOAT64,
		D_73 FLOAT64,
		P_4 FLOAT64,
		D_74 FLOAT64,
		D_75 FLOAT64,
		D_76 FLOAT64,
		B_24 FLOAT64,
		R_7 FLOAT64,
		D_77 FLOAT64,
		B_25 FLOAT64,
		B_26 FLOAT64,
		D_78 FLOAT64,
		D_79 FLOAT64,
		R_8 FLOAT64,
		R_9 FLOAT64,
		S_16 FLOAT64,
		D_80 FLOAT64,
		R_10 FLOAT64,
		R_11 FLOAT64,
		B_27 FLOAT64,
		D_81 FLOAT64,
		D_82 FLOAT64,
		S_17 FLOAT64,
		R_12 FLOAT64,
		B_28 FLOAT64,
		R_13 FLOAT64,
		D_83 FLOAT64,
		R_14 FLOAT64,
		R_15 FLOAT64,
		D_84 FLOAT64,
		R_16 FLOAT64,
		B_29 FLOAT64,
		B_30 FLOAT64,
		S_18 FLOAT64,
		D_86 FLOAT64,
		D_87 FLOAT64,
		R_17 FLOAT64,
		R_18 FLOAT64,
		D_88 FLOAT64,
		B_31 INT64,
		S_19 FLOAT64,
		R_19 FLOAT64,
		B_32 FLOAT64,
		S_20 FLOAT64,
		R_20 FLOAT64,
		R_21 FLOAT64,
		B_33 FLOAT64,
		D_89 FLOAT64,
		R_22 FLOAT64,
		R_23 FLOAT64,
		D_91 FLOAT64,
		D_92 FLOAT64,
		D_93 FLOAT64,
		D_94 FLOAT64,
		R_24 FLOAT64,
		R_25 FLOAT64,
		D_96 FLOAT64,
		S_22 FLOAT64,
		S_23 FLOAT64,
		S_24 FLOAT64,
		S_25 FLOAT64,
		S_26 FLOAT64,
		D_102 FLOAT64,
		D_103 FLOAT64,
		D_104 FLOAT64,
		D_105 FLOAT64,
		D_106 FLOAT64,
		D_107 FLOAT64,
		B_36 FLOAT64,
		B_37 FLOAT64,
		R_26 FLOAT64,
		R_27 FLOAT64,
		B_38 FLOAT64,
		D_108 FLOAT64,
		D_109 FLOAT64,
		D_110 FLOAT64,
		D_111 FLOAT64,
		B_39 FLOAT64,
		D_112 FLOAT64,
		B_40 FLOAT64,
		S_27 FLOAT64,
		D_113 FLOAT64,
		D_114 FLOAT64,
		D_115 FLOAT64,
		D_116 FLOAT64,
		D_117 FLOAT64,
		D_118 FLOAT64,
		D_119 FLOAT64,
		D_120 FLOAT64,
		D_121 FLOAT64,
		D_122 FLOAT64,
		D_123 FLOAT64,
		D_124 FLOAT64,
		D_125 FLOAT64,
		D_126 FLOAT64,
		D_127 FLOAT64,
		D_128 FLOAT64,
		D_129 FLOAT64,
		B_41 FLOAT64,
		B_42 FLOAT64,
		D_130 FLOAT64,
		D_131 FLOAT64,
		D_132 FLOAT64,
		D_133 FLOAT64,
		R_28 FLOAT64,
		D_134 FLOAT64,
		D_135 FLOAT64,
		D_136 FLOAT64,
		D_137 FLOAT64,
		D_138 FLOAT64,
		D_139 FLOAT64,
		D_140 FLOAT64,
		D_141 FLOAT64,
		D_142 FLOAT64,
		D_143 FLOAT64,
		D_144 FLOAT64,
		D_145 FLOAT64

)
OPTIONS(
skip_leading_rows=1,
format="CSV",
uris=["gs://nature_labs/train/train_data.csv"]
);
