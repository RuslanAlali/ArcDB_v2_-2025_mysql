ALTER TABLE VCF_detection ADD PARTITION (
    PARTITION p_chr22_0 VALUES IN ('chr22_0'),
    PARTITION p_chr22_1 VALUES IN ('chr22_1'),
    PARTITION p_chr22_2 VALUES IN ('chr22_2'),
    PARTITION p_chr22_3 VALUES IN ('chr22_3'),
    PARTITION p_chr22_4 VALUES IN ('chr22_4'),
    PARTITION p_chr22_5 VALUES IN ('chr22_5'),
    PARTITION p_chr22_6 VALUES IN ('chr22_6'),
    PARTITION p_chr22_7 VALUES IN ('chr22_7'),
    PARTITION p_chr22_8 VALUES IN ('chr22_8'),
    PARTITION p_chr22_9 VALUES IN ('chr22_9'),
    PARTITION p_chr22_10 VALUES IN ('chr22_10')
);