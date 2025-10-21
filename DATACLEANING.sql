-- DATA CLEANING 

select * from layoffs;


CREATE TABLE layoffs_staging LIKE layoffs;

select * from layoffs_staging;

INSERT INTO layoffs_staging select * from layoffs;



-- REMOVE DUPLICATE

select *, 
row_number() OVER(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
 from layoffs_staging;
 
 with duplicate_cte as 
 (
 select *, 
row_number() OVER(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
 from layoffs_staging
)
 select * from duplicate_cte where row_num>1;


select * from layoffs_staging where company='Casper';
select * from layoffs_staging where company='100 Thieves';


 with duplicate_cte as 
 (
 select *, 
row_number() OVER(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
 from layoffs_staging2
)
 DELETE  from duplicate_cte where row_num>1;




 
 
 CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

 
 select * from layoffs_staging3;
 
 insert into layoffs_staging3 select *, 
row_number() OVER(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
 from layoffs_staging;
 
 delete from layoffs_staging3 where row_num > 1;
 select* from layoffs_staging3 where row_num>1;
 
 
 -- STANDARDIZE THE DATA 
 
 -- company
 select company,trim(company) from  layoffs_staging3;
 
 update layoffs_staging3 set company=trim(company);
 
 -- industry
select distinct industry from  layoffs_staging3 order by 1;
select * from layoffs_staging3 where
 industry like 'Crypto%';
 update layoffs_staging3 set industry='Crypto' where industry like 'Crypto%';
 select distinct industry from layoffs_staging3 ;

-- location 
select distinct location from layoffs_staging3 order by 1;

-- country
select distinct country from layoffs_staging3 order by 1;
select distinct country,TRIM(TRAILING '.' FROM country) from layoffs_staging3 order by 1;
update layoffs_staging3 set country=TRIM(TRAILING '.' FROM country) where country like 'United state%';
update layoffs_staging3 set country='United state' where country like 'United state%';

 select `date`,
 str_to_date(`date`, '%m/%d/%Y')
 from layoffs_staging3;
 
 update layoffs_staging3 set `date`=str_to_date(`date`, '%m/%d/%Y');
 
 select * from layoffs_staging3;
 
 alter table layoffs_staging3 modify column `date` date;
 
 -- BLANK OR NULL VALUE 
 
 select * from layoffs_staging3 where total_laid_off is null and percentage_laid_off is null;
 
 select *from layoffs_staging3 where industry is null or industry='';
 
 select * from layoffs_staging3 where company like 'Bally%';
 
 update layoffs_staging3 
 set industry=null 
 where industry='';
 
 select t1.industry, t2.industry from layoffs_staging3 t1 
 join layoffs_staging3 t2 
      on t1.company=t2.company   
where (t1.industry is null or t1.industry='')
and t2.industry is not null ;

update layoffs_staging3 t1 
join layoffs_staging3 t2 on t1.company=t2.company 
set t1.industry=t2.industry
 where t1.industry is null and t2.industry is not null;
 
 


select * from layoffs_staging3;

alter table layoffs_staging3 drop row_num;

 delete from layoffs_staging3 where total_laid_off is null and percentage_laid_off is null;
 
 select* from layoffs_staging3;