file = "1000_i_L2_200.txt" ;
wSize = 25;

data = readmatrix(file);
cps = []; %% Suggested change points

acp = [193,396,590,810]; %% Actual change points

for i=1:size(data,1)
    p = data(i);
    flag = true;
    for j=1:size(cps,2)
        if(abs(p-cps(j)) < wSize)
            flag = false;
            break
        end
    end
    if flag == true
        cps(end+1) = p;
    end
end

s_c = 10 ;
for i=1:s_c
    fprintf("Suggestion %d is %d\n",i,cps(i));
end

n = 4;
tp = 0;
pr = zeros(size(acp,2));
for i=1:n
    p = cps(i);
    flag = false;
    for j=1:size(acp,2)
        if(abs(p-acp(j)) < wSize) && (pr(j) == 0)
            flag = true;
            pr(j) = 1;
            break
        end
    end
    if(flag == true)
        tp = tp +1;
    end
end
recall = tp/size(acp,2);
precision = tp/n;
f1 = 2*recall*precision/(recall+precision);
fprintf("Recall = %.2f, Precision = %.2f, F1 = %.2f,  @%d\n",recall,precision,f1,n);


