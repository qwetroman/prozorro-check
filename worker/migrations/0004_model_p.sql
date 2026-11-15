-- прогноз моделі: імовірність, що чернетку буде схвалено (0..1), і сума під ризиком (для порядку показу)
ALTER TABLE posts ADD COLUMN model_p REAL;
ALTER TABLE posts ADD COLUMN money REAL;
