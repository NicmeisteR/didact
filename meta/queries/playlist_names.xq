jsoniq version "1.0";

import module namespace file = "http://expath.org/ns/file";

let $playlists := jn:parse-json(file:read-text("./meta/data/playlists.json"))."ContentItems"

return [
    for $playlist in $playlists[]
    where $playlist."Type" = "HW2Playlist"
    order by $playlist."View"."HW2Playlist"."MaxPartySize"
    return [
        $playlist."View"."Identity",
        $playlist."View"."HW2Playlist"."DisplayInfo"."View"."HW2PlaylistDisplayInfo"."Name",
        $playlist."View"."HW2Playlist"."MaxPartySize"
    ]
]
