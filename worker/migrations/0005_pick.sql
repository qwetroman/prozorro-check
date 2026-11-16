-- як чернетку обрано для показу: top (за пріоритетом) або random (випадкова з нижчих, для чесної оцінки моделі)
ALTER TABLE posts ADD COLUMN pick TEXT;
