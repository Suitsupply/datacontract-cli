-- Create the table
CREATE TABLE `my_table` (
    `field_one` VARCHAR(10) PRIMARY KEY,
    `field_two` INT NOT NULL,
    `field_three` DATETIME
);

-- Insert the data
INSERT INTO `my_table` (`field_one`, `field_two`, `field_three`) VALUES
    ('CX-263-DU', 50, '2023-06-16 13:12:56'),
    ('IK-894-MN', 47, '2023-10-08 22:40:57'),
    ('ER-399-JY', 22, '2023-05-16 01:08:22'),
    ('MT-939-FH', 63, '2023-03-15 05:15:21'),
    ('LV-849-MI', 33, '2023-09-08 20:08:43'),
    ('VS-079-OH', 85, '2023-04-15 00:50:32'),
    ('DN-297-XY', 79, '2023-11-08 12:55:42'),
    ('ZE-172-FP', 14, '2023-12-03 18:38:38'),
    ('ID-840-EG', 89, '2023-10-02 17:17:58'),
    ('FK-230-KZ', 64, '2023-11-27 15:21:48');
