-- Comments!
CREATE TABLE person
(
    uuid uuid NOT NULL PRIMARY KEY,
    name varchar,
    dob date
);

-- More!
CREATE TABLE address
(
    uuid uuid NOT NULL PRIMARY KEY,
    street varchar, -- Inline
    city varchar,
    state varchar,
    zip varchar
);