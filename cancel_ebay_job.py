import lmslib

creds = {
            "name": "Visionary Auto Parts",
            "type": "ebay",
            "max_qty": 5,
            "last_record_number": 1146,
            "rate": 1.19,
            "domain": "api.ebay.com",
            "developer_key": "ca8c37b1-599d-4a7e-94c0-1e9227ecf080",
            "application_key": "AaronJam-devnizer-PRD-3bff6a2f3-cd1c4c33",
            "certificate_key": "PRD-bff6a2f34a86-306a-4cec-804b-8e9c",
            "auth_token": "AgAAAA**AQAAAA**aAAAAA**6GXjWA**nY+sHZ2PrBmdj6wVnY+sEZ2PrA2dj6AHloejAZeCpg+dj6x9nY+seQ**lXsDAA**AAMAAA**qHczvt78g6xRW92SoNlu2BLwVUp6p0RHgSOIenSrQrYU7k2VCKDqFK2vVcz+M8StLVLOmrwzrUCrlB28UbIeB3vcgHegqaqXggRuH+Uf0/8mPT2HRa5zny3ByJJF03MGPaet2I5+86AkrP2t9X288fkI6Wkb5vpveKXXisTzTwkk3nLXnoqYN8Z69eByR11EszsAM0oIdbgA0tUPhrQlUfvqvfdBN2SLXZIlIc29oz/Lk9Y35qKIX2PXIqXAwexNJVJPUwPGKTdY9jNcLicX6YqzmxArpJd+I3hlcYZ/P3sXHGICxqizWGiGfafMQfiNi2iNGIo9sylu/L1dtVbZzDi0dj1/YZKMVRjZQFijFtEAZ9KjW7/jD/ZIONcyQWwVoFVkxKx9EpUbD7dOGFI0h/eMUX1VMjPK300M7hdIGnsxASbeicPrV+smJRlb/8g2LfRyZ4FKlBBwyYgqL6H1kCiRwRuR6mkl3ojN11K0djK9e4GRmcqd9rIw031mo08twdLaMR5Dxwfg4qY+YjJuVVRdJe3lhFlJJqHpBgNiMCyHYPUahotcRxHi9WMSc4oS0albBHKyo1wzRDzoleVIRM6uH0pAnxdmlYHCIaI2zlv9uZrPVJC0TkEDl5SrL+XZng4jdjcnIjPK4/ZmO/nTrUTgxywW0JtpqCYzjIhZh7XyaI2I0Mb4/kO0tCwX9eVc/QS2vsUshdNEEz/p83MlukEStM5XC0CN10KWOvq5aIg3nsHSz64BKnvhZy7l64tg"
        }


def main():
    environment = lmslib.PRODUCTION
    get_jobs = lmslib.GetJobs(environment, creds)
    # get_jobs.buildRequest(jobType='SetShipmentTrackingInfo', jobStatus='Created')
    # get_jobs.buildRequest(jobType='SetShipmentTrackingInfo', jobStatus='InProcess')
    get_jobs.buildRequest(jobType='SetShipmentTrackingInfo', jobStatus='Failed')
    # get_jobs.buildRequest(jobType='SetShipmentTrackingInfo', jobStatus='Scheduled')
    response = get_jobs.sendRequest()
    response, resp_struct = get_jobs.getResponse()
    for r in resp_struct:
        abort_job = lmslib.AbortJob(environment, creds)
        abort_job.buildRequest(r['jobId'])
        response = abort_job.sendRequest()
        response, resp_struct = abort_job.getResponse()
        print response, resp_struct
    pass


if __name__ == "__main__":
    main()
