

exports.handler = async (event) => {
    const value = event.strValue
    const optParam = event.optParam ? event.optParam : "defaultValue"
    return {
        available: true,
        listValue: [100, 100],
        resultURI: "s3://mybucket/XXXXX.pdf"
    }
}
