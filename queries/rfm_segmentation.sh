db.HotelData.aggregate([
    {
        $addFields: {
            d_Recency: {
                $add: [
                    { $multiply: [0.1, { $toInt: "$DaysSinceLastStay" }] },
                    { $multiply: [0.5, { $toInt: "$AverageLeadTime" }] }
                ]
            },
            d_Frequency: {
                $add: [
                    { $multiply: [0.4, { $toInt: "$DaysSinceFirstStay" }] },
                    { $multiply: [0.5, { $toInt: "$BookingsCanceled" }] },
                    { $multiply: [0.3, { $toInt: "$BookingsCheckedIn" }] },
                    { $multiply: [0.35, { $toInt: "$BookingsNoShowed" }] }
                ]
            },
            d_Monetization: {
                $add: [
                    { $multiply: [0.4, { $toDouble: "$LodgingRevenue" }] },
                    { $multiply: [0.7, { $toDouble: "$OtherRevenue" }] }
                ]
            },
            customer_rank: {
                $add: [
                    { $multiply: [0.6, { $subtract: [1, "$d_Recency"] }] },
                    { $multiply: [0.2, "$d_Frequency"] },
                    { $multiply: [0.2, "$d_Monetization"] }
                ]
            }
        }
    },
    {
        $addFields: {
            customer_rank: {
                $add: [
                    { $multiply: [1, "$d_Frequency"] },
                    { $multiply: [1, "$d_Monetization"] },
                    { $multiply: [-1, "$d_Recency"] }
                ]
            }
        }
    },
    {
        $sort: { customer_rank: 1 }
    },
    { 
        $setWindowFields: {
            sortBy: { customer_rank: 1 },
            output: { sorted_row_number: { $documentNumber: {} } }
        }
    },
    {
        $addFields: {
            Segment_Rank: {
                $switch: {
                    branches: [
                        { case: { $lte: ["$sorted_row_number", { $multiply: [0.2, 80000] }] }, then: 1 },
                        { case: { $lte: ["$sorted_row_number", { $multiply: [0.4, 80000] }] }, then: 2 },
                        { case: { $lte: ["$sorted_row_number", { $multiply: [0.6, 80000] }] }, then: 3 },
                        { case: { $lte: ["$sorted_row_number", { $multiply: [0.8, 80000] }] }, then: 4 },
                        { case: true, then: 5 }
                    ]
                }
            }
        }
    },
    {
        $project: {
            DocIdHash: 0,
            NameHash: 0
        }
    }
]);