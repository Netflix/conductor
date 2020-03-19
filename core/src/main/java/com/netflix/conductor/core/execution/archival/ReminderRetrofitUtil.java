package com.netflix.conductor.core.execution.archival;

import com.netflix.conductor.core.execution.exceptions.BadRequestException;
import com.netflix.conductor.core.execution.exceptions.ResourceNotFoundException;
import com.netflix.conductor.core.execution.exceptions.ServiceException;
import com.netflix.conductor.core.execution.tasks.ApiInstrumentationUtil;
import io.prometheus.client.Histogram;
import okhttp3.OkHttpClient;
import okhttp3.ResponseBody;
import org.apache.http.HttpResponse;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import javax.inject.Singleton;

import static com.netflix.conductor.core.execution.tasks.ApiInstrumentationUtil.requestInProgress;
import static com.netflix.conductor.core.execution.tasks.ApiInstrumentationUtil.requestTotal;

@Singleton
public class ReminderRetrofitUtil {

    public static Retrofit getDefaultRetrofitObject(OkHttpClient httpClient, String host) {
        return new Retrofit.Builder()
                .baseUrl(host)
                .addConverterFactory(JacksonConverterFactory.create())
                .client(httpClient)
                .build();
    }

    public static <T> RetrofitResponse executeCall(Call<T> retrofitCall, String methodName) {

        Response<T> execute = null;
        String error;
        Histogram.Timer timer = ApiInstrumentationUtil.requestLatency.labels(methodName).startTimer();
        requestInProgress.labels(methodName).inc();

        try {

            execute = retrofitCall.execute();

            if (execute.isSuccessful()) {
                requestTotal.labels(methodName,
                        ApiInstrumentationUtil.ApiResponse.SUCCESS.name(), execute.message(), String.valueOf(execute.code())).inc();

                return new RetrofitResponse(execute.code(), execute.body());
            }

            error = execute.errorBody().string();
            requestTotal.labels(methodName, ApiInstrumentationUtil.ApiResponse.FAILURE.name(), execute.message(), String.valueOf(execute.code())).inc();
        } catch (Exception exception) {

            requestTotal.labels(methodName, ApiInstrumentationUtil.ApiResponse.FAILURE.name(), "", execute != null ? String.valueOf(execute.code()) : "500").inc();
            requestInProgress.labels(methodName).dec();
            timer.observeDuration();
            throw new ServiceException(exception);
        } finally {

            requestInProgress.labels(methodName).dec();
            timer.observeDuration();
        }

        if (execute.code() == 400) {
            throw new BadRequestException(error);
        }
        if (execute.code() == 404) {
            throw new ResourceNotFoundException(error);
        }
        throw new ServiceException(error);
    }
}
