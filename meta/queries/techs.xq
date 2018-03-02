jsoniq version "1.0";

import module namespace file = "http://expath.org/ns/file";

let $techs_1 := jn:parse-json(file:read-text("./meta/data/techs_1.json"))."ContentItems"
let $techs_2 := jn:parse-json(file:read-text("./meta/data/techs_2.json"))."ContentItems"
let $techs := [$techs_1[], $techs_2[]]

return [
    for $tech in $techs[]
    where $tech."Type" = "HW2Tech"
    order by $tech."View"."HW2Tech"."ObjectTypeId"
    return [
        $tech."View"."Identity",
        $tech."View"."HW2Tech"."ObjectTypeId"
    ]
]
